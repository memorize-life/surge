// Package mocks contains various mocks useful for testing.
package mocks

import (
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/service/glacier"
	"github.com/aws/aws-sdk-go-v2/service/glacier/glacieriface"
)

// Glacier is a thread-safe Amazon Glacier client mock.
type Glacier struct {
	glacieriface.GlacierAPI

	CallCount uint32

	InitiateMultipartUploadRequestMock func() glacier.InitiateMultipartUploadRequest
	ListPartsRequestMock               func() glacier.ListPartsRequest
	UploadMultipartPartRequestMock     func() glacier.UploadMultipartPartRequest
	CompleteMultipartUploadRequestMock func() glacier.CompleteMultipartUploadRequest
	DescribeJobRequestMock             func() glacier.DescribeJobRequest
	GetJobOutputRequestMock            func() glacier.GetJobOutputRequest
}

// InitiateMultipartUploadRequest returns a mocked request value for making API operation for Amazon Glacier.
// It calls InitiateMultipartUploadRequestMock if set and returns uninitialized InitiateMultipartUploadRequest otherwise.
// Calling this method increases CallCount.
func (g *Glacier) InitiateMultipartUploadRequest(*glacier.InitiateMultipartUploadInput) glacier.InitiateMultipartUploadRequest {
	atomic.AddUint32(&g.CallCount, 1)
	if g.InitiateMultipartUploadRequestMock != nil {
		return g.InitiateMultipartUploadRequestMock()
	}
	return glacier.InitiateMultipartUploadRequest{}
}

// ListPartsRequest returns a mocked request value for making API operation for Amazon Glacier.
// It calls ListPartsRequestMock if set and returns uninitialized ListPartsRequest otherwise.
// Calling this method increases CallCount.
func (g *Glacier) ListPartsRequest(*glacier.ListPartsInput) glacier.ListPartsRequest {
	atomic.AddUint32(&g.CallCount, 1)
	if g.ListPartsRequestMock != nil {
		return g.ListPartsRequestMock()
	}
	return glacier.ListPartsRequest{}
}

// UploadMultipartPartRequest returns a mocked request value for making API operation for Amazon Glacier.
// It calls UploadMultipartPartRequestMock if set and returns uninitialized UploadMultipartPartRequest otherwise.
// Calling this method increases CallCount.
func (g *Glacier) UploadMultipartPartRequest(*glacier.UploadMultipartPartInput) glacier.UploadMultipartPartRequest {
	atomic.AddUint32(&g.CallCount, 1)
	if g.UploadMultipartPartRequestMock != nil {
		return g.UploadMultipartPartRequestMock()
	}
	return glacier.UploadMultipartPartRequest{}
}

// CompleteMultipartUploadRequest returns a mocked request value for making API operation for Amazon Glacier.
// It calls CompleteMultipartUploadRequestMock if set and returns uninitialized CompleteMultipartUploadRequest otherwise.
// Calling this method increases CallCount.
func (g *Glacier) CompleteMultipartUploadRequest(*glacier.CompleteMultipartUploadInput) glacier.CompleteMultipartUploadRequest {
	atomic.AddUint32(&g.CallCount, 1)
	if g.CompleteMultipartUploadRequestMock != nil {
		return g.CompleteMultipartUploadRequestMock()
	}
	return glacier.CompleteMultipartUploadRequest{}
}

// DescribeJobRequest returns a mocked request value for making API operation for Amazon Glacier.
// It calls DescribeJobRequestMock if set and returns uninitialized DescribeJobRequest otherwise.
// Calling this method increases CallCount.
func (g *Glacier) DescribeJobRequest(input *glacier.DescribeJobInput) glacier.DescribeJobRequest {
	atomic.AddUint32(&g.CallCount, 1)
	if g.DescribeJobRequestMock != nil {
		return g.DescribeJobRequestMock()
	}
	return glacier.DescribeJobRequest{}
}

// GetJobOutputRequest returns a mocked request value for making API operation for Amazon Glacier.
// It calls GetJobOutputRequestMock if set and returns uninitialized GetJobOutputRequest otherwise.
// Calling this method increases CallCount.
func (g *Glacier) GetJobOutputRequest(input *glacier.GetJobOutputInput) glacier.GetJobOutputRequest {
	atomic.AddUint32(&g.CallCount, 1)
	if g.GetJobOutputRequestMock != nil {
		return g.GetJobOutputRequestMock()
	}
	return glacier.GetJobOutputRequest{}
}
