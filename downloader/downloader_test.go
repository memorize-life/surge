package downloader

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/31z4/surge/mocks"
	"github.com/31z4/surge/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glacier"
)

func newTestInput() *Input {
	return &Input{
		AccountId: "test_account",
		VaultName: "test_vault",
		FileName:  "test_file",
		JobId:     "test_job",
		PartSize:  123,
	}
}

func TestCheckJob(t *testing.T) {
	t.Run("send error", func(t *testing.T) {
		err := errors.New("test")
		requestMock := func() glacier.DescribeJobRequest {
			return glacier.DescribeJobRequest{
				Request: &aws.Request{
					Error: err,
				},
			}
		}
		mock := &mocks.Glacier{
			DescribeJobRequestMock: requestMock,
		}

		input := newTestInput()
		downloader := New(mock, input)

		if got := downloader.checkJob(); got != err {
			t.Fatalf("got %#v, want %#v", got, err)
		}
	})

	t.Run("unsupported action", func(t *testing.T) {
		action := glacier.ActionCode("test")
		requestMock := func() glacier.DescribeJobRequest {
			return glacier.DescribeJobRequest{
				Request: &aws.Request{
					Data: &glacier.DescribeJobOutput{
						Action: action,
					},
				},
			}
		}
		mock := &mocks.Glacier{
			DescribeJobRequestMock: requestMock,
		}

		input := newTestInput()
		downloader := New(mock, input)
		errString := "test action is not supported"

		if got := downloader.checkJob(); got.Error() != errString {
			t.Fatalf("got %#v, want %#v", got, errString)
		}
	})

	t.Run("not succeeded", func(t *testing.T) {
		action := glacier.ActionCode("ArchiveRetrieval")
		status := glacier.StatusCode("InProgress")
		requestMock := func() glacier.DescribeJobRequest {
			return glacier.DescribeJobRequest{
				Request: &aws.Request{
					Data: &glacier.DescribeJobOutput{
						Action: action,
						StatusCode: status,
					},
				},
			}
		}
		mock := &mocks.Glacier{
			DescribeJobRequestMock: requestMock,
		}

		input := newTestInput()
		downloader := New(mock, input)
		errString := "the job is not succeeded yet"

		if got := downloader.checkJob(); got.Error() != errString {
			t.Fatalf("got %#v, want %#v", got, errString)
		}
	})

	t.Run("failed", func(t *testing.T) {
		action := glacier.ActionCode("ArchiveRetrieval")
		status := glacier.StatusCode("Failed")
		message := "test"
		requestMock := func() glacier.DescribeJobRequest {
			return glacier.DescribeJobRequest{
				Request: &aws.Request{
					Data: &glacier.DescribeJobOutput{
						Action: action,
						StatusCode: status,
						StatusMessage: &message,
					},
				},
			}
		}
		mock := &mocks.Glacier{
			DescribeJobRequestMock: requestMock,
		}

		input := newTestInput()
		downloader := New(mock, input)
		errString := "the job is failed: " + message

		if got := downloader.checkJob(); got.Error() != errString {
			t.Fatalf("got %#v, want %#v", got, errString)
		}
	})

	t.Run("unexpected status", func(t *testing.T) {
		action := glacier.ActionCode("ArchiveRetrieval")
		status := glacier.StatusCode("test")
		requestMock := func() glacier.DescribeJobRequest {
			return glacier.DescribeJobRequest{
				Request: &aws.Request{
					Data: &glacier.DescribeJobOutput{
						Action: action,
						StatusCode: status,
					},
				},
			}
		}
		mock := &mocks.Glacier{
			DescribeJobRequestMock: requestMock,
		}

		input := newTestInput()
		downloader := New(mock, input)
		errString := "job status is unexpected: " + string(status)

		if got := downloader.checkJob(); got.Error() != errString {
			t.Fatalf("got %#v, want %#v", got, errString)
		}
	})

	t.Run("invalid range", func(t *testing.T) {
		action := glacier.ActionCode("ArchiveRetrieval")
		status := glacier.StatusCode("Succeeded")
		requestMock := func() glacier.DescribeJobRequest {
			return glacier.DescribeJobRequest{
				Request: &aws.Request{
					Data: &glacier.DescribeJobOutput{
						Action: action,
						StatusCode: status,
					},
				},
			}
		}
		mock := &mocks.Glacier{
			DescribeJobRequestMock: requestMock,
		}

		input := newTestInput()
		downloader := New(mock, input)
		errString := "the retrieved range must be tree-hash aligned"

		if got := downloader.checkJob(); got.Error() != errString {
			t.Fatalf("got %#v, want %#v", got, errString)
		}
	})

	t.Run("ok", func(t *testing.T) {
		action := glacier.ActionCode("ArchiveRetrieval")
		status := glacier.StatusCode("Succeeded")
		hash := "test"
		var size int64 = 123
		requestMock := func() glacier.DescribeJobRequest {
			return glacier.DescribeJobRequest{
				Request: &aws.Request{
					Data: &glacier.DescribeJobOutput{
						Action: action,
						StatusCode: status,
						ArchiveSizeInBytes: &size,
						SHA256TreeHash: &hash,
					},
				},
			}
		}
		mock := &mocks.Glacier{
			DescribeJobRequestMock: requestMock,
		}

		input := newTestInput()
		downloader := New(mock, input)

		if err := downloader.checkJob(); err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		if downloader.size != size {
			t.Fatalf("unexpected size: %d", downloader.size)
		}

		if *downloader.treeHash != hash {
			t.Fatalf("unexpected treeHash: %s", *downloader.treeHash)
		}
	})
}

func TestOpenFile(t *testing.T) {
	t.Run("existing file", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}

		defer os.Remove(file.Name())

		input := &Input{
			FileName: file.Name(),
		}
		downloader := &Downloader{
			input: input,
		}

		if err := downloader.openFile(); err == nil {
			t.Fatalf("got nil, want error")
		}
	})

	t.Run("existing directory", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.RemoveAll(dir)

		input := &Input{
			FileName: dir,
		}
		downloader := &Downloader{
			input: input,
		}

		if err := downloader.openFile(); err == nil {
			t.Fatal("got nil, want error")
		}
	})

	t.Run("ok", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.RemoveAll(dir)

		input := &Input{
			FileName: path.Join(dir, "test"),
		}
		downloader := &Downloader{
			input: input,
		}

		if err := downloader.openFile(); err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		defer downloader.file.Close()

		if downloader.file == nil {
			t.Fatal("file must not be nil")
		}
	})
}

func TestDownloadPart(t *testing.T) {
	t.Run("send error", func(t *testing.T) {
		err := errors.New("test")
		requestMock := func() glacier.GetJobOutputRequest {
			return glacier.GetJobOutputRequest{
				Request: &aws.Request{
					Error: err,
				},
			}
		}
		mock := &mocks.Glacier{
			GetJobOutputRequestMock: requestMock,
		}

		input := newTestInput()
		downloader := New(mock, input)
		r := &utils.Range{}

		if got := downloader.downloadPart(r); got != err {
			t.Fatalf("got %#v, want %#v", got, err)
		}
	})

	t.Run("read error", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}

		defer os.Remove(file.Name())

		requestMock := func() glacier.GetJobOutputRequest {
			return glacier.GetJobOutputRequest{
				Request: &aws.Request{
					Data: &glacier.GetJobOutputOutput{
						Body: file,
					},
				},
			}
		}
		mock := &mocks.Glacier{
			GetJobOutputRequestMock: requestMock,
		}

		input := newTestInput()
		downloader := New(mock, input)
		r := &utils.Range{}

		err = downloader.downloadPart(r)
		if err == nil {
			t.Fatal("got nil, want error")
		}

		if _, ok := err.(*os.PathError); !ok {
			t.Fatalf("got %T, want *os.PathError", err)
		}
	})

	t.Run("size mismatch", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		requestMock := func() glacier.GetJobOutputRequest {
			return glacier.GetJobOutputRequest{
				Request: &aws.Request{
					Data: &glacier.GetJobOutputOutput{
						Body: file,
					},
				},
			}
		}
		mock := &mocks.Glacier{
			GetJobOutputRequestMock: requestMock,
		}

		input := newTestInput()
		downloader := New(mock, input)
		r := &utils.Range{
			Offset: 0,
			Limit: 123,
		}

		err = downloader.downloadPart(r)
		if err == nil {
			t.Fatal("got nil, want error")
		}

		errString := "size mismatch"
		if got := err.Error(); got != errString {
			t.Fatalf("got %q, want %q", got, errString)
		}
	})

	t.Run("hash error", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		checksum := "test"
		requestMock := func() glacier.GetJobOutputRequest {
			return glacier.GetJobOutputRequest{
				Request: &aws.Request{
					Data: &glacier.GetJobOutputOutput{
						Body: file,
						Checksum: &checksum,
					},
				},
			}
		}
		mock := &mocks.Glacier{
			GetJobOutputRequestMock: requestMock,
		}

		input := newTestInput()
		downloader := New(mock, input)
		r := &utils.Range{}

		err = downloader.downloadPart(r)
		if err == nil {
			t.Fatal("got nil, want error")
		}

		errString := "could not compute hash"
		if got := err.Error(); got != errString {
			t.Fatalf("got %q, want %q", got, errString)
		}
	})

	t.Run("hash mismatch", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		data := []byte{'t', 'e', 's', 't'}
		if err := ioutil.WriteFile(file.Name(), data, 0644); err != nil {
			t.Fatal(err)
		}

		checksum := "test"
		requestMock := func() glacier.GetJobOutputRequest {
			return glacier.GetJobOutputRequest{
				Request: &aws.Request{
					Data: &glacier.GetJobOutputOutput{
						Body: file,
						Checksum: &checksum,
					},
				},
			}
		}
		mock := &mocks.Glacier{
			GetJobOutputRequestMock: requestMock,
		}

		input := newTestInput()
		downloader := New(mock, input)
		r := &utils.Range{
			Offset: 0,
			Limit: 4,
		}

		err = downloader.downloadPart(r)
		if err == nil {
			t.Fatal("got nil, want error")
		}

		errString := "hash mismatch"
		if got := err.Error(); got != errString {
			t.Fatalf("got %q, want %q", got, errString)
		}
	})

	t.Run("write error", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		requestMock := func() glacier.GetJobOutputRequest {
			return glacier.GetJobOutputRequest{
				Request: &aws.Request{
					Data: &glacier.GetJobOutputOutput{
						Body: file,
					},
				},
			}
		}
		mock := &mocks.Glacier{
			GetJobOutputRequestMock: requestMock,
		}

		input := newTestInput()
		downloader := New(mock, input)
		r := &utils.Range{}

		err = downloader.downloadPart(r)
		if err == nil {
			t.Fatal("got nil, want error")
		}

		errString := "invalid argument"
		if got := err.Error(); got != errString {
			t.Fatalf("got %q, want %q", got, errString)
		}
	})

	t.Run("ok", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.RemoveAll(dir)

		data := []byte{'t', 'e', 's', 't'}
		filename := path.Join(dir, "in")

		if err := ioutil.WriteFile(filename, data, 0644); err != nil {
			t.Fatal(err)
		}
		file, err := os.Open(filename)
		if err != nil {
			t.Fatal(err)
		}

		defer file.Close()

		requestMock := func() glacier.GetJobOutputRequest {
			return glacier.GetJobOutputRequest{
				Request: &aws.Request{
					Data: &glacier.GetJobOutputOutput{
						Body: file,
					},
				},
			}
		}
		mock := &mocks.Glacier{
			GetJobOutputRequestMock: requestMock,
		}

		input := newTestInput()
		input.FileName = path.Join(dir, "out")

		downloader := New(mock, input)
		r := &utils.Range{
			Offset: 0,
			Limit: 4,
		}

		if err := downloader.openFile(); err != nil {
			t.Fatal(err)
		}

		if err := downloader.downloadPart(r); err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
}

func TestMultipartDownload(t *testing.T) {
	t.Run("download error", func(t *testing.T) {
		err := errors.New("test")
		requestMock := func() glacier.GetJobOutputRequest {
			return glacier.GetJobOutputRequest{
				Request: &aws.Request{
					Error: err,
				},
			}
		}
		mock := &mocks.Glacier{
			GetJobOutputRequestMock: requestMock,
		}

		input := newTestInput()
		input.PartSize = 4

		downloader := New(mock, input)
		downloader.size = 11

		downloader.multipartDownload(2)

		if mock.CallCount != 3 {
			t.Fatalf("unexpected mock call count: %d", mock.CallCount)
		}
	})

	t.Run("ok", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.RemoveAll(dir)

		data := []byte{'t', 'e', 's', 't'}
		filename := path.Join(dir, "in")

		if err := ioutil.WriteFile(filename, data, 0644); err != nil {
			t.Fatal(err)
		}
		file, err := os.Open(filename)
		if err != nil {
			t.Fatal(err)
		}

		defer file.Close()

		requestMock := func() glacier.GetJobOutputRequest {
			return glacier.GetJobOutputRequest{
				Request: &aws.Request{
					Data: &glacier.GetJobOutputOutput{
						Body: file,
					},
				},
			}
		}
		mock := &mocks.Glacier{
			GetJobOutputRequestMock: requestMock,
		}

		input := newTestInput()
		input.PartSize = 4
		input.FileName = path.Join(dir, "out")

		downloader := New(mock, input)
		downloader.size = 4

		if err := downloader.openFile(); err != nil {
			t.Fatal(err)
		}

		downloader.multipartDownload(4)

		if mock.CallCount != 1 {
			t.Fatalf("unexpected mock call count: %d", mock.CallCount)
		}

		content, err := ioutil.ReadFile(input.FileName)
		if err != nil {
			t.Fatal(err)
		}

		if string(content) != "test" {
			t.Fatalf("got %q, want \"test\"", content)
		}
	})
}

func TestCheckTreeHash(t *testing.T) {
	t.Run("hash error", func(t *testing.T) {
		downloader := &Downloader{}

		err := downloader.checkTreeHash()
		if err == nil {
			t.Fatal("got nil, want error")
		}

		errString := "could not compute hash"
		if got := err.Error(); got != errString {
			t.Fatalf("got %q, want %q", got, errString)
		}
	})

	t.Run("hash mismatch", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		data := []byte{'t', 'e', 's', 't'}
		if err := ioutil.WriteFile(file.Name(), data, 0644); err != nil {
			t.Fatal(err)
		}

		hash := ""
		downloader := &Downloader{
			file: file,
			treeHash: &hash,
		}

		err = downloader.checkTreeHash()
		if err == nil {
			t.Fatal("got nil, want error")
		}

		errString := "hash mismatch"
		if got := err.Error(); got != errString {
			t.Fatalf("got %q, want %q", got, errString)
		}
	})

	t.Run("ok", func(t *testing.T) {
		file, err := ioutil.TempFile("", "surge")
		if err != nil {
			t.Fatal(err)
		}

		defer os.Remove(file.Name())
		defer file.Close()

		data := []byte{'t', 'e', 's', 't'}
		if err := ioutil.WriteFile(file.Name(), data, 0644); err != nil {
			t.Fatal(err)
		}

		hash := "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
		downloader := &Downloader{
			file: file,
			treeHash: &hash,
		}

		if err := downloader.checkTreeHash(); err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
}
