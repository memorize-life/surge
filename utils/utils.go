package utils

import (
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/glacier"
)

type Range struct {
	Offset int64
	Limit  int64
}

func (r *Range) String() string {
	return fmt.Sprint(r.Offset, "-", r.Offset+r.Limit-1)
}

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

func ComputeTreeHash(r io.ReadSeeker) *string {
	treeHash := glacier.ComputeHashes(r).TreeHash
	if treeHash == nil {
		return nil
	}

	encoded := hex.EncodeToString(treeHash)
	return &encoded
}
