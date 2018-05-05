// Package downloader implements Amazon Glacier multipart download.
//
// For information about Glacier, see https://aws.amazon.com/glacier/.
package downloader

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"

	"github.com/31z4/surge/pkg/utils"
	"github.com/aws/aws-sdk-go-v2/service/glacier"
	"github.com/aws/aws-sdk-go-v2/service/glacier/glacieriface"
)

// Input provides options for multipart download from an Amazon Glacier vault.
type Input struct {
	// The AccountId value is the AWS account ID of the account that owns the vault.
	// You can either specify an AWS account ID or optionally a single '-' (hyphen),
	// in which case Amazon Glacier uses the AWS account ID associated with the
	// credentials used to sign the request. If you use an account ID, do not include
	// any hyphens ('-') in the ID.
	AccountId string

	// The name of the vault.
	VaultName string

	// Filename where the content will be saved.
	FileName string

	// The job ID whose data is downloaded.
	JobId string

	// The size of each part except the last, in bytes. The last part can be smaller
	// than this part size.
	PartSize int64
}

// Downloader holds internal downloader state.
type Downloader struct {
	service glacieriface.GlacierAPI
	input   *Input

	file     *os.File
	treeHash *string
	size     int64
	offset   int64
}

// New creates a new instance of the downloader with a service and input.
func New(service glacieriface.GlacierAPI, input *Input) *Downloader {
	return &Downloader{
		service: service,
		input:   input,
	}
}

func (d *Downloader) openFile() error {
	file, err := os.OpenFile(
		d.input.FileName,
		os.O_RDWR|os.O_CREATE|os.O_EXCL,
		0644,
	)
	if err != nil {
		return err
	}

	d.file = file

	return nil
}

func (d *Downloader) downloadPart(r *utils.Range) error {
	rangeString := fmt.Sprint("bytes=", r)
	input := &glacier.GetJobOutputInput{
		AccountId: &d.input.AccountId,
		JobId:     &d.input.JobId,
		Range:     &rangeString,
		VaultName: &d.input.VaultName,
	}

	request := d.service.GetJobOutputRequest(input)
	result, err := request.Send()
	if err != nil {
		return err
	}

	// This might be not memory efficient for large parts.
	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		return err
	}

	if len(body) != int(r.Limit) {
		return errors.New("size mismatch")
	}

	if result.Checksum != nil {
		reader := bytes.NewReader(body)
		treeHash := utils.ComputeTreeHash(reader)
		if treeHash == nil {
			return errors.New("could not compute hash")
		}

		if *result.Checksum != *treeHash {
			return errors.New("hash mismatch")
		}
	}

	n, err := d.file.WriteAt(body, r.Offset)
	if err != nil {
		return err
	}
	if n != int(r.Limit) {
		return fmt.Errorf("could not write %d bytes to the file", r.Limit)
	}

	return nil
}

func (d *Downloader) getNextRange() *utils.Range {
	if d.offset >= d.size {
		return nil
	}

	offset := d.offset
	d.offset += d.input.PartSize

	limit := d.input.PartSize
	if offset+limit > d.size {
		limit = d.size - offset
	}

	return &utils.Range{
		Offset: offset,
		Limit:  limit,
	}
}

func (d *Downloader) multipartDownload(jobs int) {
	parts := make(chan *utils.Range)

	var wg sync.WaitGroup
	wg.Add(jobs)

	for i := 0; i < jobs; i++ {
		go func() {
			defer wg.Done()

			for p := range parts {
				log.Printf("start downloading part (%v)", p)
				if err := d.downloadPart(p); err != nil {
					log.Printf("error downloading part (%v): %v", p, err)
				} else {
					log.Printf("finish downloading part (%v)", p)
				}
			}
		}()
	}

	for {
		if p := d.getNextRange(); p != nil {
			parts <- p
		} else {
			break
		}
	}

	close(parts)
	wg.Wait()
}

func (d *Downloader) checkJob() error {
	input := &glacier.DescribeJobInput{
		AccountId: &d.input.AccountId,
		JobId:     &d.input.JobId,
		VaultName: &d.input.VaultName,
	}

	request := d.service.DescribeJobRequest(input)
	result, err := request.Send()
	if err != nil {
		return err
	}

	action := string(result.Action)
	if action != "ArchiveRetrieval" {
		return errors.New(action + " action is not supported")
	}

	status := string(result.StatusCode)
	if status != "Succeeded" {
		if status == "InProgress" {
			return errors.New("the job is not succeeded yet")
		}
		if status == "Failed" {
			return errors.New("the job is failed: " + *result.StatusMessage)
		}
		return errors.New("job status is unexpected: " + status)
	}

	if result.SHA256TreeHash == nil {
		return errors.New("the retrieved range must be tree-hash aligned")
	}

	d.size = *result.ArchiveSizeInBytes
	d.treeHash = result.SHA256TreeHash

	return nil
}

func (d *Downloader) checkTreeHash() error {
	treeHash := utils.ComputeTreeHash(d.file)
	if treeHash == nil {
		return errors.New("could not compute hash")
	}

	if *treeHash != *d.treeHash {
		return errors.New("hash mismatch")
	}

	return nil
}

// Download performs parallel multipart download.
// The maximum number of the parallel downloads is limited by the jobs parameter.
func (d Downloader) Download(jobs int) error {
	if err := d.checkJob(); err != nil {
		return err
	}

	if err := d.openFile(); err != nil {
		return err
	}
	defer d.file.Close()

	if err := os.Truncate(d.input.FileName, d.size); err != nil {
		return err
	}

	d.multipartDownload(jobs)

	if err := d.checkTreeHash(); err != nil {
		return err
	}

	return nil
}
