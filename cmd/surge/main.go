package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/31z4/surge/pkg/uploader"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/glacier"
)

func main() {
	flag.Usage = func() {
		const usage = "Usage: surge [options] VAULT FILE\n\n" +
			"Upload the file to the existing Amazon Glacier vault in multiple parts\n\n" +
			"Options:\n"
		fmt.Fprint(flag.CommandLine.Output(), usage)
		flag.PrintDefaults()
		os.Exit(2)
	}

	profile := flag.String("profile", "", "use a specific AWS profile")
	accountId := flag.String("account-id", "-", "the AWS account ID of the account that owns the vault")
	partSize := flag.Int64("part-size", 1048576, "the size of each part except the last, in bytes")
	uploadId := flag.String("upload-id", "", "the upload ID of the multipart upload")
	jobs := flag.Int("jobs", runtime.GOMAXPROCS(0), "the maximum number of the parallel uploads")

	flag.Parse()
	args := flag.Args()

	if len(args) != 2 {
		flag.Usage()
	}

	vaultName, fileName := args[0], args[1]

	var configs external.Configs
	if *profile != "" {
		configs = append(configs, external.WithSharedConfigProfile(*profile))
	}

	config, err := external.LoadDefaultAWSConfig(configs...)
	if err != nil {
		log.Fatal(err.Error())
	}

	service := glacier.New(config)
	input := &uploader.Input{
		AccountId: *accountId,
		PartSize:  *partSize,
		VaultName: vaultName,
		FileName:  fileName,
		UploadId:  *uploadId,
	}

	uploader := uploader.New(service, input)

	if err := uploader.Upload(*jobs); err != nil {
		log.Fatal(err.Error())
	}
}
