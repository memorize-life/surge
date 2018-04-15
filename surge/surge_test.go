package surge

import (
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glacier"
	"github.com/aws/aws-sdk-go-v2/service/glacier/glacieriface"
	"github.com/pkg/errors"
)

func TestRangeFromString(t *testing.T) {
	cases := map[string]struct {
		input  string
		output *contentRange
	}{
		"empty": {
			input:  "",
			output: nil,
		},
		"no dash": {
			input:  "test",
			output: nil,
		},
		"two dashes": {
			input:  "0-1-test",
			output: nil,
		},
		"begin is not int": {
			input:  "test-0",
			output: nil,
		},
		"end is not int": {
			input:  "0-test",
			output: nil,
		},
		"begin is greater than end": {
			input:  "1-0",
			output: nil,
		},

		"begin is equal to end": {
			input:  "0-0",
			output: &contentRange{offset: 0, limit: 1},
		},
		"begin is less than end": {
			input:  "0-1",
			output: &contentRange{offset: 0, limit: 2},
		},
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			got := rangeFromString(&test.input)
			failed := false

			if test.output != nil {
				if *got != *test.output {
					failed = true
				}
			} else {
				if got != test.output {
					failed = true
				}
			}

			if failed {
				t.Errorf("got %#v, want %#v", got, test.output)
			}
		})
	}
}

func newTestUploadInput() *UploadInput {
	return &UploadInput{
		AccountId: "test_account",
		VaultName: "test_vault",
		FileName:  "test_file",
		UploadId:  "test_id",
		PartSize:  123,
	}
}

func TestOpenFile(t *testing.T) {
	t.Run("nonexistent", func(t *testing.T) {
		input := newTestUploadInput()
		input.FileName = "nonexistent"
		uploader := Surge{
			input: input,
		}
		errString := "open nonexistent: no such file or directory"

		if got := uploader.openFile(); got.Error() != errString {
			t.Errorf("got %#v, want %#v", got, errString)
		}
	})

	t.Run("directory", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "surge")
		if err != nil {
			t.Error(err)
		}

		defer os.RemoveAll(dir)

		input := newTestUploadInput()
		input.FileName = dir

		uploader := Surge{
			input: input,
		}
		errString := "directories are not supported"

		if got := uploader.openFile(); got.Error() != errString {
			t.Errorf("got %#v, want %#v", got, errString)
		}
	})

	t.Run("ok", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Error(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		if _, err := file.WriteString("test"); err != nil {
			t.Error(err)
		}

		input := newTestUploadInput()
		input.FileName = file.Name()

		uploader := Surge{
			input: input,
		}

		if err := uploader.openFile(); err != nil {
			t.Errorf("unexpected error: %#v", err)
		}

		if uploader.size != 4 {
			t.Errorf("unexpected size: %#v", uploader.size)
		}
	})
}

type glacierMock struct {
	glacieriface.GlacierAPI

	callCount uint32

	initiateMultipartUploadRequest func() glacier.InitiateMultipartUploadRequest
	listPartsRequest               func() glacier.ListPartsRequest
	uploadMultipartPartRequest     func() glacier.UploadMultipartPartRequest
	completeMultipartUploadRequest func() glacier.CompleteMultipartUploadRequest
}

func (m *glacierMock) InitiateMultipartUploadRequest(*glacier.InitiateMultipartUploadInput) glacier.InitiateMultipartUploadRequest {
	atomic.AddUint32(&m.callCount, 1)
	if m.initiateMultipartUploadRequest != nil {
		return m.initiateMultipartUploadRequest()
	}
	return glacier.InitiateMultipartUploadRequest{}
}

func (m *glacierMock) ListPartsRequest(*glacier.ListPartsInput) glacier.ListPartsRequest {
	atomic.AddUint32(&m.callCount, 1)
	if m.listPartsRequest != nil {
		return m.listPartsRequest()
	}
	return glacier.ListPartsRequest{}
}

func (m *glacierMock) UploadMultipartPartRequest(*glacier.UploadMultipartPartInput) glacier.UploadMultipartPartRequest {
	atomic.AddUint32(&m.callCount, 1)
	if m.uploadMultipartPartRequest != nil {
		return m.uploadMultipartPartRequest()
	}
	return glacier.UploadMultipartPartRequest{}
}

func (m *glacierMock) CompleteMultipartUploadRequest(*glacier.CompleteMultipartUploadInput) glacier.CompleteMultipartUploadRequest {
	atomic.AddUint32(&m.callCount, 1)
	if m.completeMultipartUploadRequest != nil {
		return m.completeMultipartUploadRequest()
	}
	return glacier.CompleteMultipartUploadRequest{}
}

func TestInitiateUpload(t *testing.T) {
	t.Run("does nothing", func(t *testing.T) {
		mock := &glacierMock{}
		uploader := New(mock, newTestUploadInput())

		if err := uploader.initiateUpload(); err != nil {
			t.Errorf("unexpected error: %#v", err)
		}
	})

	t.Run("send error", func(t *testing.T) {
		err := errors.New("test")
		mock := &glacierMock{
			initiateMultipartUploadRequest: func() glacier.InitiateMultipartUploadRequest {
				return glacier.InitiateMultipartUploadRequest{
					Request: &aws.Request{
						Error: err,
					},
				}
			},
		}
		input := newTestUploadInput()
		input.UploadId = ""
		uploader := New(mock, input)

		if got := uploader.initiateUpload(); got != err {
			t.Errorf("got %#v, want %#v", got, err)
		}
	})

	t.Run("sends", func(t *testing.T) {
		uploadId := "test_id"
		mock := &glacierMock{
			initiateMultipartUploadRequest: func() glacier.InitiateMultipartUploadRequest {
				return glacier.InitiateMultipartUploadRequest{
					Request: &aws.Request{
						Data: &glacier.InitiateMultipartUploadOutput{
							UploadId: &uploadId,
						},
					},
				}
			},
		}
		input := newTestUploadInput()
		input.UploadId = ""
		uploader := New(mock, input)

		if err := uploader.initiateUpload(); err != nil {
			t.Errorf("unexpected error: %#v", err)
		}

		if uploader.input.UploadId != uploadId {
			t.Errorf("got %#v, want %#v", uploader.input.UploadId, uploadId)
		}
	})
}

func TestCheckPart(t *testing.T) {
	t.Run("invalid range", func(t *testing.T) {
		uploader := Surge{}
		errString := "part (test) range is invalid"
		part := &glacier.PartListElement{
			RangeInBytes: aws.String("test"),
		}

		if ok, got := uploader.checkPart(part); got.Error() != errString {
			t.Errorf("got %#v, want %#v", got, errString)
		} else if ok {
			t.Errorf("unexpected ok: %#v", ok)
		}
	})

	t.Run("file size mismatch", func(t *testing.T) {
		uploader := Surge{
			size: 1,
		}
		errString := "file size mismatch"
		part := &glacier.PartListElement{
			RangeInBytes: aws.String("1-1"),
		}

		if ok, got := uploader.checkPart(part); got.Error() != errString {
			t.Errorf("got %#v, want %#v", got, errString)
		} else if ok {
			t.Errorf("unexpected ok: %#v", ok)
		}
	})

	t.Run("hashing error", func(t *testing.T) {
		uploader := Surge{
			size: 1,
		}
		errString := "could not compute hashes of part (0-0)"
		part := &glacier.PartListElement{
			RangeInBytes: aws.String("0-0"),
		}

		if ok, got := uploader.checkPart(part); got.Error() != errString {
			t.Errorf("got %#v, want %#v", got, errString)
		} else if ok {
			t.Errorf("unexpected ok: %#v", ok)
		}
	})

	t.Run("ok", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Error(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		if _, err := file.WriteString("test"); err != nil {
			t.Error(err)
		}

		uploader := Surge{
			file:     file,
			size:     4,
			uploaded: make(map[int64]struct{}),
		}
		part := &glacier.PartListElement{
			RangeInBytes:   aws.String("0-3"),
			SHA256TreeHash: aws.String("9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"),
		}

		if ok, err := uploader.checkPart(part); err != nil {
			t.Errorf("unexpected error: %#v", err)
		} else if !ok {
			t.Errorf("expected ok")
		}

		if _, exists := uploader.uploaded[0]; !exists {
			t.Errorf("the part was not added to uploaded")
		}
	})

	t.Run("not ok", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Error(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		if _, err := file.WriteString("test"); err != nil {
			t.Error(err)
		}

		uploader := Surge{
			file:     file,
			size:     4,
			uploaded: make(map[int64]struct{}),
		}
		part := &glacier.PartListElement{
			RangeInBytes:   aws.String("0-3"),
			SHA256TreeHash: aws.String("test_hash"),
		}

		if ok, err := uploader.checkPart(part); err != nil {
			t.Errorf("unexpected error: %#v", err)
		} else if ok {
			t.Errorf("not expected ok")
		}

		if _, exists := uploader.uploaded[0]; exists {
			t.Errorf("the part added to uploaded")
		}
	})
}

func newListPartsRequestMock(r *aws.Request) glacier.ListPartsRequest {
	return glacier.ListPartsRequest{
		Copy: func(*glacier.ListPartsInput) glacier.ListPartsRequest {
			return glacier.ListPartsRequest{
				Request: r,
			}
		},
	}
}

func TestCheckUploadedParts(t *testing.T) {
	t.Run("list parts error", func(t *testing.T) {
		err := errors.New("test")
		request := aws.Request{
			Error: err,
		}
		mock := &glacierMock{
			listPartsRequest: func() glacier.ListPartsRequest {
				return newListPartsRequestMock(&request)
			},
		}
		uploader := New(mock, newTestUploadInput())

		if got := uploader.checkUploadedParts(); got != err {
			t.Errorf("got %#v, want %#v", got, err)
		}
	})

	t.Run("part size mismatch", func(t *testing.T) {
		var partSize int64 = 321
		request := aws.Request{
			Data: &glacier.ListPartsOutput{
				PartSizeInBytes: &partSize,
			},
			Operation: &aws.Operation{},
		}
		mock := &glacierMock{
			listPartsRequest: func() glacier.ListPartsRequest {
				return newListPartsRequestMock(&request)
			},
		}
		uploader := New(mock, newTestUploadInput())
		errString := "part size mismatch"

		if got := uploader.checkUploadedParts(); got.Error() != errString {
			t.Errorf("got %#v, want %#v", got, errString)
		}
	})

	t.Run("no parts", func(t *testing.T) {
		input := newTestUploadInput()
		request := aws.Request{
			Data: &glacier.ListPartsOutput{
				PartSizeInBytes: &input.PartSize,
			},
			Operation: &aws.Operation{},
		}
		mock := &glacierMock{
			listPartsRequest: func() glacier.ListPartsRequest {
				return newListPartsRequestMock(&request)
			},
		}
		uploader := New(mock, input)

		if err := uploader.checkUploadedParts(); err != nil {
			t.Errorf("unexpected error: %#v", err)
		}
	})

	t.Run("check part error", func(t *testing.T) {
		input := newTestUploadInput()
		request := aws.Request{
			Data: &glacier.ListPartsOutput{
				PartSizeInBytes: &input.PartSize,
				Parts: []glacier.PartListElement{
					{RangeInBytes: aws.String("test")},
				},
			},
			Operation: &aws.Operation{},
		}
		mock := &glacierMock{
			listPartsRequest: func() glacier.ListPartsRequest {
				return newListPartsRequestMock(&request)
			},
		}
		uploader := New(mock, input)
		errString := "part (test) range is invalid"

		if got := uploader.checkUploadedParts(); got.Error() != errString {
			t.Errorf("got %#v, want %#v", got, errString)
		}
	})
}

func TestUploadPart(t *testing.T) {
	t.Run("hashing error", func(t *testing.T) {
		uploader := Surge{}
		r := &contentRange{}
		errString := "could not compute hashes"

		if got := uploader.uploadPart(r); got.Error() != errString {
			t.Errorf("got %#v, want %#v", got, errString)
		}
	})

	t.Run("upload error", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Error(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		if _, err := file.WriteString("test"); err != nil {
			t.Error(err)
		}

		err = errors.New("test")
		mock := &glacierMock{
			uploadMultipartPartRequest: func() glacier.UploadMultipartPartRequest {
				return glacier.UploadMultipartPartRequest{
					Request: &aws.Request{
						Error: err,
					},
				}
			},
		}

		input := newTestUploadInput()
		input.FileName = file.Name()

		uploader := &Surge{
			service: mock,
			input:   input,
			file:    file,
			size:    4,
		}

		r := &contentRange{
			offset: 0,
			limit:  uploader.size,
		}

		if got := uploader.uploadPart(r); got != err {
			t.Errorf("got %#v, want %#v", got, err)
		}
	})

	t.Run("ok", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Error(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		if _, err := file.WriteString("test"); err != nil {
			t.Error(err)
		}

		mock := &glacierMock{
			uploadMultipartPartRequest: func() glacier.UploadMultipartPartRequest {
				return glacier.UploadMultipartPartRequest{
					Request: &aws.Request{
						Data: &glacier.UploadMultipartPartOutput{},
					},
				}
			},
		}

		input := newTestUploadInput()
		input.FileName = file.Name()

		uploader := &Surge{
			service: mock,
			input:   input,
			file:    file,
			size:    4,
		}

		r := &contentRange{
			offset: 0,
			limit:  uploader.size,
		}

		if err := uploader.uploadPart(r); err != nil {
			t.Errorf("unexpected error: %#v", err)
		}
	})
}

func TestMultipartUpload(t *testing.T) {
	t.Run("upload error", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Error(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		if _, err := file.WriteString("test_upload"); err != nil {
			t.Error(err)
		}

		err = errors.New("test")
		mock := &glacierMock{
			uploadMultipartPartRequest: func() glacier.UploadMultipartPartRequest {
				return glacier.UploadMultipartPartRequest{
					Request: &aws.Request{
						Error: err,
					},
				}
			},
		}

		input := newTestUploadInput()
		input.FileName = file.Name()
		input.PartSize = 4

		uploader := &Surge{
			service: mock,
			input:   input,
			file:    file,
			size:    11,
		}

		uploader.multipartUpload(2)

		if mock.callCount != 3 {
			t.Errorf("unexpected mock call count: %d", mock.callCount)
		}
	})

	t.Run("ok", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Error(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		if _, err := file.WriteString("test_upload"); err != nil {
			t.Error(err)
		}

		mock := &glacierMock{
			uploadMultipartPartRequest: func() glacier.UploadMultipartPartRequest {
				return glacier.UploadMultipartPartRequest{
					Request: &aws.Request{
						Data: &glacier.UploadMultipartPartOutput{},
					},
				}
			},
		}

		input := newTestUploadInput()
		input.FileName = file.Name()
		input.PartSize = 2

		uploader := &Surge{
			service: mock,
			input:   input,
			file:    file,
			size:    11,
		}

		uploader.multipartUpload(2)

		if mock.callCount != 6 {
			t.Errorf("unexpected mock call count: %d", mock.callCount)
		}
	})
}

func TestCompleteUpload(t *testing.T) {
	t.Run("hashing error", func(t *testing.T) {
		uploader := Surge{}
		errString := "could not compute hashes"

		if result, got := uploader.completeUpload(); got.Error() != errString {
			t.Errorf("got %#v, want %#v", got, errString)
		} else if result != nil {
			t.Errorf("unexpected result: %#v", result)
		}
	})

	t.Run("complete error", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Error(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		if _, err := file.WriteString("test_upload"); err != nil {
			t.Error(err)
		}

		err = errors.New("test")
		mock := &glacierMock{
			completeMultipartUploadRequest: func() glacier.CompleteMultipartUploadRequest {
				return glacier.CompleteMultipartUploadRequest{
					Request: &aws.Request{
						Error: err,
					},
				}
			},
		}

		input := newTestUploadInput()
		input.FileName = file.Name()

		uploader := &Surge{
			service: mock,
			input:   input,
			file:    file,
			size:    11,
		}

		if result, got := uploader.completeUpload(); got != err {
			t.Errorf("got %#v, want %#v", got, err)
		} else if result != nil {
			t.Errorf("unexpected result: %#v", result)
		}
	})

	t.Run("ok", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Error(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		if _, err := file.WriteString("test_upload"); err != nil {
			t.Error(err)
		}

		location := "test_location"
		mock := &glacierMock{
			completeMultipartUploadRequest: func() glacier.CompleteMultipartUploadRequest {
				return glacier.CompleteMultipartUploadRequest{
					Request: &aws.Request{
						Data: &glacier.UploadArchiveOutput{
							Location: &location,
						},
					},
				}
			},
		}

		input := newTestUploadInput()
		input.FileName = file.Name()

		uploader := &Surge{
			service: mock,
			input:   input,
			file:    file,
			size:    11,
		}

		if got, err := uploader.completeUpload(); *got != location {
			t.Errorf("got %#v, want %#v", *got, location)
		} else if err != nil {
			t.Errorf("unexpected error: %#v", err)
		}
	})
}
