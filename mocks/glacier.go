package mocks

import (
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/service/glacier"
	"github.com/aws/aws-sdk-go-v2/service/glacier/glacieriface"
)

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

func (g *Glacier) InitiateMultipartUploadRequest(*glacier.InitiateMultipartUploadInput) glacier.InitiateMultipartUploadRequest {
	atomic.AddUint32(&g.CallCount, 1)
	if g.InitiateMultipartUploadRequestMock != nil {
		return g.InitiateMultipartUploadRequestMock()
	}
	return glacier.InitiateMultipartUploadRequest{}
}

func (g *Glacier) ListPartsRequest(*glacier.ListPartsInput) glacier.ListPartsRequest {
	atomic.AddUint32(&g.CallCount, 1)
	if g.ListPartsRequestMock != nil {
		return g.ListPartsRequestMock()
	}
	return glacier.ListPartsRequest{}
}

func (g *Glacier) UploadMultipartPartRequest(*glacier.UploadMultipartPartInput) glacier.UploadMultipartPartRequest {
	atomic.AddUint32(&g.CallCount, 1)
	if g.UploadMultipartPartRequestMock != nil {
		return g.UploadMultipartPartRequestMock()
	}
	return glacier.UploadMultipartPartRequest{}
}

func (g *Glacier) CompleteMultipartUploadRequest(*glacier.CompleteMultipartUploadInput) glacier.CompleteMultipartUploadRequest {
	atomic.AddUint32(&g.CallCount, 1)
	if g.CompleteMultipartUploadRequestMock != nil {
		return g.CompleteMultipartUploadRequestMock()
	}
	return glacier.CompleteMultipartUploadRequest{}
}

func (g *Glacier) DescribeJobRequest(input *glacier.DescribeJobInput) glacier.DescribeJobRequest {
	atomic.AddUint32(&g.CallCount, 1)
	if g.DescribeJobRequestMock != nil {
		return g.DescribeJobRequestMock()
	}
	return glacier.DescribeJobRequest{}
}

func (g *Glacier) GetJobOutputRequest(input *glacier.GetJobOutputInput) glacier.GetJobOutputRequest {
	atomic.AddUint32(&g.CallCount, 1)
	if g.GetJobOutputRequestMock != nil {
		return g.GetJobOutputRequestMock()
	}
	return glacier.GetJobOutputRequest{}
}
