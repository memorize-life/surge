// Package utils contains various Amazon Glacier utility functions and structures.
package utils

import (
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/glacier"
)

// Range represents a range of bytes that is used for multipart archive upload and download.
type Range struct {
	Offset int64
	Limit  int64
}

// String returns the string representation.
func (r *Range) String() string {
	return fmt.Sprint(r.Offset, "-", r.Offset+r.Limit-1)
}

// RangeFromString constructs a Range from a string s.
// If the string doesn't represent a valid byte range nil is returned.
func RangeFromString(s *string) *Range {
	split := strings.Split(*s, "-")
	if len(split) != 2 {
		return nil
	}

	var result Range

	if begin, err := strconv.ParseInt(split[0], 10, 64); err == nil {
		result.Offset = begin
	} else {
		return nil
	}

	if end, err := strconv.ParseInt(split[1], 10, 64); err == nil {
		result.Limit = end - result.Offset + 1
	} else {
		return nil
	}

	if result.Limit <= 0 {
		return nil
	}

	return &result
}

// ComputeTreeHash computes the hex encoded tree-hash of a seekable reader r.
// If there was an error computing the hash nil is returned.
func ComputeTreeHash(r io.ReadSeeker) *string {
	treeHash := glacier.ComputeHashes(r).TreeHash
	if treeHash == nil {
		return nil
	}

	encoded := hex.EncodeToString(treeHash)
	return &encoded
}
