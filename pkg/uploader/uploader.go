// Package uploader implements Amazon Glacier multipart upload.
//
// For information about Glacier, see https://aws.amazon.com/glacier/.
package uploader

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/31z4/surge/pkg/utils"
	"github.com/aws/aws-sdk-go-v2/service/glacier"
	"github.com/aws/aws-sdk-go-v2/service/glacier/glacieriface"
	"github.com/pkg/errors"
)

// Input provides options for multipart upload to an Amazon Glacier vault.
type Input struct {
	// The AccountId value is the AWS account ID of the account that owns the vault.
	// You can either specify an AWS account ID or optionally a single '-' (hyphen),
	// in which case Amazon Glacier uses the AWS account ID associated with the
	// credentials used to sign the request. If you use an account ID, do not include
	// any hyphens ('-') in the ID.
	AccountId string

	// The name of the vault.
	VaultName string

	// The file to upload.
	FileName string

	// The upload ID of the multipart upload.
	// If the value is empty then a new upload will be initiated.
	// Specify the upload ID to resume an interrupted upload.
	UploadId string

	// The size of each part except the last, in bytes. The last part can be smaller
	// than this part size.
	PartSize int64
}

// Uploader holds internal uploader state.
type Uploader struct {
	service  glacieriface.ClientAPI
	input    *Input
	uploaded map[int64]struct{}

	file   *os.File
	size   int64
	offset int64
}

// New creates a new instance of the uploader with a service and input.
func New(service glacieriface.ClientAPI, input *Input) *Uploader {
	return &Uploader{
		service:  service,
		input:    input,
		uploaded: make(map[int64]struct{}),
	}
}

func (s *Uploader) initiateUpload() error {
	if s.input.UploadId != "" {
		return nil
	}

	partSize := strconv.FormatInt(s.input.PartSize, 10)
	input := &glacier.InitiateMultipartUploadInput{
		AccountId: &s.input.AccountId,
		PartSize:  &partSize,
		VaultName: &s.input.VaultName,
	}

	request := s.service.InitiateMultipartUploadRequest(input)
	result, err := request.Send(context.TODO())
	if err != nil {
		return err
	}

	s.input.UploadId = *result.UploadId
	return nil
}

func (s *Uploader) getNextRange() *utils.Range {
	var offset int64

	for {
		if s.offset >= s.size {
			return nil
		}

		offset = s.offset
		s.offset += s.input.PartSize

		if _, exists := s.uploaded[offset]; !exists {
			break
		}
	}

	limit := s.input.PartSize
	if offset+limit > s.size {
		limit = s.size - offset
	}

	return &utils.Range{
		Offset: offset,
		Limit:  limit,
	}
}

func (s *Uploader) openFile() error {
	file, err := os.Open(s.input.FileName)
	if err != nil {
		return err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}

	if info.IsDir() {
		file.Close()
		return errors.New("directories are not supported")
	}

	s.file = file
	s.size = info.Size()

	return nil
}

func (s *Uploader) uploadPart(r *utils.Range) error {
	body := io.NewSectionReader(s.file, r.Offset, r.Limit)
	treeHash := utils.ComputeTreeHash(body)
	if treeHash == nil {
		return errors.New("could not compute hashes")
	}

	rangeString := fmt.Sprint("bytes ", r, "/*")
	input := &glacier.UploadMultipartPartInput{
		AccountId: &s.input.AccountId,
		UploadId:  &s.input.UploadId,
		VaultName: &s.input.VaultName,
		Body:      body,
		Checksum:  treeHash,
		Range:     &rangeString,
	}

	request := s.service.UploadMultipartPartRequest(input)
	if _, err := request.Send(context.TODO()); err != nil {
		return err
	}

	return nil
}

func (s *Uploader) multipartUpload(jobs int) {
	parts := make(chan *utils.Range)

	var wg sync.WaitGroup
	wg.Add(jobs)

	for i := 0; i < jobs; i++ {
		go func() {
			defer wg.Done()

			for p := range parts {
				log.Printf("start uploading part (%v)", p)
				if err := s.uploadPart(p); err != nil {
					log.Printf("error uploading part (%v): %v", p, err)
				} else {
					log.Printf("finish uploading part (%v)", p)
				}
			}
		}()
	}

	for {
		if p := s.getNextRange(); p != nil {
			parts <- p
		} else {
			break
		}
	}

	close(parts)
	wg.Wait()
}

func (s *Uploader) checkPart(part *glacier.PartListElement) (bool, error) {
	partRange := utils.RangeFromString(part.RangeInBytes)
	if partRange == nil {
		return false, fmt.Errorf("part (%v) range is invalid", *part.RangeInBytes)
	}

	if partRange.Offset >= s.size {
		return false, errors.New("file size mismatch")
	}

	body := io.NewSectionReader(s.file, partRange.Offset, partRange.Limit)
	treeHash := utils.ComputeTreeHash(body)
	if treeHash == nil {
		return false, fmt.Errorf("could not compute hashes of part (%v)", *part.RangeInBytes)
	}

	if *treeHash == *part.SHA256TreeHash {
		s.uploaded[partRange.Offset] = struct{}{}
		return true, nil
	}
	return false, nil
}

func (s *Uploader) checkUploadedParts() error {
	log.Println("start checking uploaded parts")

	input := &glacier.ListPartsInput{
		AccountId: &s.input.AccountId,
		UploadId:  &s.input.UploadId,
		VaultName: &s.input.VaultName,
	}

	request := s.service.ListPartsRequest(input)
	pager := glacier.NewListPartsPaginator(request)

	for pager.Next(context.TODO()) {
		result := pager.CurrentPage()
		if *result.PartSizeInBytes != s.input.PartSize {
			return errors.New("part size mismatch")
		}

		for _, part := range result.Parts {
			if ok, err := s.checkPart(&part); err != nil {
				return err
			} else if ok {
				log.Printf("part (%v) is ok", *part.RangeInBytes)
			} else {
				log.Printf("part (%v) hash mismatch", *part.RangeInBytes)
			}
		}
	}

	if err := pager.Err(); err != nil {
		return err
	}

	log.Println("finish checking uploaded parts")

	return nil
}

func (s *Uploader) completeUpload() (*string, error) {
	treeHash := utils.ComputeTreeHash(s.file)
	if treeHash == nil {
		return nil, errors.New("could not compute hashes")
	}

	size := strconv.FormatInt(s.size, 10)
	input := &glacier.CompleteMultipartUploadInput{
		AccountId:   &s.input.AccountId,
		ArchiveSize: &size,
		Checksum:    treeHash,
		UploadId:    &s.input.UploadId,
		VaultName:   &s.input.VaultName,
	}

	request := s.service.CompleteMultipartUploadRequest(input)
	result, err := request.Send(context.TODO())
	if err != nil {
		return nil, err
	}

	return result.Location, nil
}

// Upload performs parallel multipart upload.
// The maximum number of the parallel uploads is limited by the jobs parameter.
func (s Uploader) Upload(jobs int) error {
	if err := s.openFile(); err != nil {
		return err
	}
	defer s.file.Close()

	if err := s.initiateUpload(); err != nil {
		return err
	}

	log.Println("upload", s.input.UploadId, "initiated")

	if err := s.checkUploadedParts(); err != nil {
		return err
	}

	s.multipartUpload(jobs)

	location, err := s.completeUpload()
	if err != nil {
		return err
	}

	log.Println("upload location is", *location)

	return nil
}
