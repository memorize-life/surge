package uploader

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/31z4/surge/mocks"
	"github.com/31z4/surge/utils"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glacier"
	"github.com/pkg/errors"
)

func newTestInput() *Input {
	return &Input{
		AccountId: "test_account",
		VaultName: "test_vault",
		FileName:  "test_file",
		UploadId:  "test_id",
		PartSize:  123,
	}
}

func TestOpenFile(t *testing.T) {
	t.Run("nonexistent", func(t *testing.T) {
		input := newTestInput()
		input.FileName = "nonexistent"
		uploader := Uploader{
			input: input,
		}
		errString := "open nonexistent: no such file or directory"

		if got := uploader.openFile(); got.Error() != errString {
			t.Fatalf("got %#v, want %#v", got, errString)
		}
	})

	t.Run("directory", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.RemoveAll(dir)

		input := newTestInput()
		input.FileName = dir

		uploader := Uploader{
			input: input,
		}
		errString := "directories are not supported"

		if got := uploader.openFile(); got.Error() != errString {
			t.Fatalf("got %#v, want %#v", got, errString)
		}
	})

	t.Run("ok", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		if _, err := file.WriteString("test"); err != nil {
			t.Fatal(err)
		}

		input := newTestInput()
		input.FileName = file.Name()

		uploader := Uploader{
			input: input,
		}

		if err := uploader.openFile(); err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		if uploader.size != 4 {
			t.Fatalf("unexpected size: %#v", uploader.size)
		}
	})
}

func TestInitiateUpload(t *testing.T) {
	t.Run("does nothing", func(t *testing.T) {
		mock := &mocks.Glacier{}
		uploader := New(mock, newTestInput())

		if err := uploader.initiateUpload(); err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("send error", func(t *testing.T) {
		err := errors.New("test")
		requestMock := func() glacier.InitiateMultipartUploadRequest {
			return glacier.InitiateMultipartUploadRequest{
				Request: &aws.Request{
					Error: err,
				},
			}
		}
		mock := &mocks.Glacier{
			InitiateMultipartUploadRequestMock: requestMock,
		}

		input := newTestInput()
		input.UploadId = ""
		uploader := New(mock, input)

		if got := uploader.initiateUpload(); got != err {
			t.Fatalf("got %#v, want %#v", got, err)
		}
	})

	t.Run("sends", func(t *testing.T) {
		uploadId := "test_id"
		requestMock := func() glacier.InitiateMultipartUploadRequest {
			return glacier.InitiateMultipartUploadRequest{
				Request: &aws.Request{
					Data: &glacier.InitiateMultipartUploadOutput{
						UploadId: &uploadId,
					},
				},
			}
		}
		mock := &mocks.Glacier{
			InitiateMultipartUploadRequestMock: requestMock,
		}

		input := newTestInput()
		input.UploadId = ""
		uploader := New(mock, input)

		if err := uploader.initiateUpload(); err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		if uploader.input.UploadId != uploadId {
			t.Fatalf("got %#v, want %#v", uploader.input.UploadId, uploadId)
		}
	})
}

func TestCheckPart(t *testing.T) {
	t.Run("invalid range", func(t *testing.T) {
		uploader := Uploader{}
		errString := "part (test) range is invalid"
		part := &glacier.PartListElement{
			RangeInBytes: aws.String("test"),
		}

		if ok, got := uploader.checkPart(part); got.Error() != errString {
			t.Fatalf("got %#v, want %#v", got, errString)
		} else if ok {
			t.Fatalf("unexpected ok: %#v", ok)
		}
	})

	t.Run("file size mismatch", func(t *testing.T) {
		uploader := Uploader{
			size: 1,
		}
		errString := "file size mismatch"
		part := &glacier.PartListElement{
			RangeInBytes: aws.String("1-1"),
		}

		if ok, got := uploader.checkPart(part); got.Error() != errString {
			t.Fatalf("got %#v, want %#v", got, errString)
		} else if ok {
			t.Fatalf("unexpected ok: %#v", ok)
		}
	})

	t.Run("hashing error", func(t *testing.T) {
		uploader := Uploader{
			size: 1,
		}
		errString := "could not compute hashes of part (0-0)"
		part := &glacier.PartListElement{
			RangeInBytes: aws.String("0-0"),
		}

		if ok, got := uploader.checkPart(part); got.Error() != errString {
			t.Fatalf("got %#v, want %#v", got, errString)
		} else if ok {
			t.Fatalf("unexpected ok: %#v", ok)
		}
	})

	t.Run("ok", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		if _, err := file.WriteString("test"); err != nil {
			t.Fatal(err)
		}

		uploader := Uploader{
			file:     file,
			size:     4,
			uploaded: make(map[int64]struct{}),
		}
		part := &glacier.PartListElement{
			RangeInBytes:   aws.String("0-3"),
			SHA256TreeHash: aws.String("9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"),
		}

		if ok, err := uploader.checkPart(part); err != nil {
			t.Fatalf("unexpected error: %#v", err)
		} else if !ok {
			t.Fatalf("expected ok")
		}

		if _, exists := uploader.uploaded[0]; !exists {
			t.Fatalf("the part was not added to uploaded")
		}
	})

	t.Run("not ok", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		if _, err := file.WriteString("test"); err != nil {
			t.Fatal(err)
		}

		uploader := Uploader{
			file:     file,
			size:     4,
			uploaded: make(map[int64]struct{}),
		}
		part := &glacier.PartListElement{
			RangeInBytes:   aws.String("0-3"),
			SHA256TreeHash: aws.String("test_hash"),
		}

		if ok, err := uploader.checkPart(part); err != nil {
			t.Fatalf("unexpected error: %#v", err)
		} else if ok {
			t.Fatalf("not expected ok")
		}

		if _, exists := uploader.uploaded[0]; exists {
			t.Fatalf("the part added to uploaded")
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
		requestMock := func() glacier.ListPartsRequest {
			return newListPartsRequestMock(&request)
		}
		mock := &mocks.Glacier{
			ListPartsRequestMock: requestMock,
		}

		uploader := New(mock, newTestInput())

		if got := uploader.checkUploadedParts(); got != err {
			t.Fatalf("got %#v, want %#v", got, err)
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
		requestMock := func() glacier.ListPartsRequest {
			return newListPartsRequestMock(&request)
		}
		mock := &mocks.Glacier{
			ListPartsRequestMock: requestMock,
		}

		uploader := New(mock, newTestInput())
		errString := "part size mismatch"

		if got := uploader.checkUploadedParts(); got.Error() != errString {
			t.Fatalf("got %#v, want %#v", got, errString)
		}
	})

	t.Run("no parts", func(t *testing.T) {
		input := newTestInput()
		request := aws.Request{
			Data: &glacier.ListPartsOutput{
				PartSizeInBytes: &input.PartSize,
			},
			Operation: &aws.Operation{},
		}
		requestMock := func() glacier.ListPartsRequest {
			return newListPartsRequestMock(&request)
		}
		mock := &mocks.Glacier{
			ListPartsRequestMock: requestMock,
		}

		uploader := New(mock, input)

		if err := uploader.checkUploadedParts(); err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("check part error", func(t *testing.T) {
		input := newTestInput()
		request := aws.Request{
			Data: &glacier.ListPartsOutput{
				PartSizeInBytes: &input.PartSize,
				Parts: []glacier.PartListElement{
					{RangeInBytes: aws.String("test")},
				},
			},
			Operation: &aws.Operation{},
		}
		requestMock := func() glacier.ListPartsRequest {
			return newListPartsRequestMock(&request)
		}
		mock := &mocks.Glacier{
			ListPartsRequestMock: requestMock,
		}

		uploader := New(mock, input)
		errString := "part (test) range is invalid"

		if got := uploader.checkUploadedParts(); got.Error() != errString {
			t.Fatalf("got %#v, want %#v", got, errString)
		}
	})
}

func TestUploadPart(t *testing.T) {
	t.Run("hashing error", func(t *testing.T) {
		uploader := Uploader{}
		r := &utils.Range{}
		errString := "could not compute hashes"

		if got := uploader.uploadPart(r); got.Error() != errString {
			t.Fatalf("got %#v, want %#v", got, errString)
		}
	})

	t.Run("upload error", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		if _, err := file.WriteString("test"); err != nil {
			t.Fatal(err)
		}

		err = errors.New("test")
		requestMock := func() glacier.UploadMultipartPartRequest {
			return glacier.UploadMultipartPartRequest{
				Request: &aws.Request{
					Error: err,
				},
			}
		}
		mock := &mocks.Glacier{
			UploadMultipartPartRequestMock: requestMock,
		}

		input := newTestInput()
		input.FileName = file.Name()

		uploader := &Uploader{
			service: mock,
			input:   input,
			file:    file,
			size:    4,
		}

		r := &utils.Range{
			Offset: 0,
			Limit:  uploader.size,
		}

		if got := uploader.uploadPart(r); got != err {
			t.Fatalf("got %#v, want %#v", got, err)
		}
	})

	t.Run("ok", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		if _, err := file.WriteString("test"); err != nil {
			t.Fatal(err)
		}

		requestMock := func() glacier.UploadMultipartPartRequest {
			return glacier.UploadMultipartPartRequest{
				Request: &aws.Request{
					Data: &glacier.UploadMultipartPartOutput{},
				},
			}
		}
		mock := &mocks.Glacier{
			UploadMultipartPartRequestMock: requestMock,
		}

		input := newTestInput()
		input.FileName = file.Name()

		uploader := &Uploader{
			service: mock,
			input:   input,
			file:    file,
			size:    4,
		}

		r := &utils.Range{
			Offset: 0,
			Limit:  uploader.size,
		}

		if err := uploader.uploadPart(r); err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
}

func TestMultipartUpload(t *testing.T) {
	t.Run("upload error", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		if _, err := file.WriteString("test_upload"); err != nil {
			t.Fatal(err)
		}

		err = errors.New("test")
		requestMock := func() glacier.UploadMultipartPartRequest {
			return glacier.UploadMultipartPartRequest{
				Request: &aws.Request{
					Error: err,
				},
			}
		}
		mock := &mocks.Glacier{
			UploadMultipartPartRequestMock: requestMock,
		}

		input := newTestInput()
		input.FileName = file.Name()
		input.PartSize = 4

		uploader := &Uploader{
			service: mock,
			input:   input,
			file:    file,
			size:    11,
		}

		uploader.multipartUpload(2)

		if mock.CallCount != 3 {
			t.Fatalf("unexpected mock call count: %d", mock.CallCount)
		}
	})

	t.Run("ok", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		if _, err := file.WriteString("test_upload"); err != nil {
			t.Fatal(err)
		}

		requestMock := func() glacier.UploadMultipartPartRequest {
			return glacier.UploadMultipartPartRequest{
				Request: &aws.Request{
					Data: &glacier.UploadMultipartPartOutput{},
				},
			}
		}
		mock := &mocks.Glacier{
			UploadMultipartPartRequestMock: requestMock,
		}

		input := newTestInput()
		input.FileName = file.Name()
		input.PartSize = 2

		uploader := &Uploader{
			service: mock,
			input:   input,
			file:    file,
			size:    11,
		}

		uploader.multipartUpload(2)

		if mock.CallCount != 6 {
			t.Fatalf("unexpected mock call count: %d", mock.CallCount)
		}
	})
}

func TestCompleteUpload(t *testing.T) {
	t.Run("hashing error", func(t *testing.T) {
		uploader := Uploader{}
		errString := "could not compute hashes"

		if result, got := uploader.completeUpload(); got.Error() != errString {
			t.Fatalf("got %#v, want %#v", got, errString)
		} else if result != nil {
			t.Fatalf("unexpected result: %#v", result)
		}
	})

	t.Run("complete error", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		if _, err := file.WriteString("test_upload"); err != nil {
			t.Fatal(err)
		}

		err = errors.New("test")
		requestMock := func() glacier.CompleteMultipartUploadRequest {
			return glacier.CompleteMultipartUploadRequest{
				Request: &aws.Request{
					Error: err,
				},
			}
		}
		mock := &mocks.Glacier{
			CompleteMultipartUploadRequestMock: requestMock,
		}

		input := newTestInput()
		input.FileName = file.Name()

		uploader := &Uploader{
			service: mock,
			input:   input,
			file:    file,
			size:    11,
		}

		if result, got := uploader.completeUpload(); got != err {
			t.Fatalf("got %#v, want %#v", got, err)
		} else if result != nil {
			t.Fatalf("unexpected result: %#v", result)
		}
	})

	t.Run("ok", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		if _, err := file.WriteString("test_upload"); err != nil {
			t.Fatal(err)
		}

		location := "test_location"
		requestMock := func() glacier.CompleteMultipartUploadRequest {
			return glacier.CompleteMultipartUploadRequest{
				Request: &aws.Request{
					Data: &glacier.UploadArchiveOutput{
						Location: &location,
					},
				},
			}
		}
		mock := &mocks.Glacier{
			CompleteMultipartUploadRequestMock: requestMock,
		}

		input := newTestInput()
		input.FileName = file.Name()

		uploader := &Uploader{
			service: mock,
			input:   input,
			file:    file,
			size:    11,
		}

		if got, err := uploader.completeUpload(); *got != location {
			t.Fatalf("got %#v, want %#v", *got, location)
		} else if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
}
