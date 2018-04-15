// Package surge implements Amazon Glacier multipart upload.
//
// For information about Glacier, see https://aws.amazon.com/glacier/.
package surge

import (
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/glacier"
	"github.com/aws/aws-sdk-go-v2/service/glacier/glacieriface"
	"github.com/pkg/errors"
)

// UploadInput provides options for multipart upload to an Amazon Glacier vault.
type UploadInput struct {
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

// Surge holds internal uploader state.
type Surge struct {
	service  glacieriface.GlacierAPI
	input    *UploadInput
	uploaded map[int64]struct{}

	file   *os.File
	size   int64
	offset int64
}

// New creates a new instance of the Surge uploader with a service and input.
func New(service glacieriface.GlacierAPI, input *UploadInput) *Surge {
	return &Surge{
		service:  service,
		input:    input,
		uploaded: make(map[int64]struct{}),
	}
}

type contentRange struct {
	offset int64
	limit  int64
}

func (r *contentRange) String() string {
	return fmt.Sprint(r.offset, "-", r.offset+r.limit-1)
}

func rangeFromString(s *string) *contentRange {
	split := strings.Split(*s, "-")
	if len(split) != 2 {
		return nil
	}

	var result contentRange

	if begin, err := strconv.ParseInt(split[0], 10, 64); err == nil {
		result.offset = begin
	} else {
		return nil
	}

	if end, err := strconv.ParseInt(split[1], 10, 64); err == nil {
		result.limit = end - result.offset + 1
	} else {
		return nil
	}

	if result.limit <= 0 {
		return nil
	}

	return &result
}

func (s *Surge) initiateUpload() error {
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
	result, err := request.Send()
	if err != nil {
		return err
	}

	s.input.UploadId = *result.UploadId
	return nil
}

func (s *Surge) getNextRange() *contentRange {
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

	return &contentRange{offset, limit}
}

func (s *Surge) openFile() error {
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

func (s *Surge) uploadPart(r *contentRange) error {
	body := io.NewSectionReader(s.file, r.offset, r.limit)
	treeHash := s.computeTreeHash(body)
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
	if _, err := request.Send(); err != nil {
		return err
	}

	return nil
}

func (s *Surge) multipartUpload(jobs int) {
	parts := make(chan *contentRange)

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

func (s *Surge) computeTreeHash(r io.ReadSeeker) *string {
	treeHash := glacier.ComputeHashes(r).TreeHash
	if treeHash == nil {
		return nil
	}

	encoded := hex.EncodeToString(treeHash)
	return &encoded
}

func (s *Surge) checkPart(part *glacier.PartListElement) (bool, error) {
	partRange := rangeFromString(part.RangeInBytes)
	if partRange == nil {
		return false, fmt.Errorf("part (%v) range is invalid", *part.RangeInBytes)
	}

	if partRange.offset >= s.size {
		return false, errors.New("file size mismatch")
	}

	body := io.NewSectionReader(s.file, partRange.offset, partRange.limit)
	treeHash := s.computeTreeHash(body)
	if treeHash == nil {
		return false, fmt.Errorf("could not compute hashes of part (%v)", *part.RangeInBytes)
	}

	if *treeHash == *part.SHA256TreeHash {
		s.uploaded[partRange.offset] = struct{}{}
		return true, nil
	}
	return false, nil
}

func (s *Surge) checkUploadedParts() error {
	log.Println("start checking uploaded parts")

	input := &glacier.ListPartsInput{
		AccountId: &s.input.AccountId,
		UploadId:  &s.input.UploadId,
		VaultName: &s.input.VaultName,
	}

	request := s.service.ListPartsRequest(input)
	pager := request.Paginate()

	for pager.Next() {
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

func (s *Surge) completeUpload() (*string, error) {
	treeHash := s.computeTreeHash(s.file)
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
	result, err := request.Send()
	if err != nil {
		return nil, err
	}

	return result.Location, nil
}

// Upload performs parallel multipart upload.
// The maximum number of the parallel uploads is limited by the jobs parameter.
func (s Surge) Upload(jobs int) error {
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
