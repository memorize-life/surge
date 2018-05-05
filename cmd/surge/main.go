package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/31z4/surge/pkg/downloader"
	"github.com/31z4/surge/pkg/uploader"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/glacier"
)

func main() {
	flag.Usage = func() {
		const (
			usage = "Usage: surge [options] <command>\n\n" +
				"Amazon Glacier multipart download and upload\n\n" +
				"Options:\n"
			commands = "\nCommands:\n" +
				"  download   Download a retrieved archive\n" +
				"  upload     Upload an archive to the existing vault\n"
		)

		fmt.Fprint(flag.CommandLine.Output(), usage)
		flag.PrintDefaults()
		fmt.Fprint(flag.CommandLine.Output(), commands)

		os.Exit(2)
	}

	uploadCommand := flag.NewFlagSet("upload", flag.ExitOnError)
	uploadCommand.Usage = func() {
		const usage = "Usage: surge upload [options] VAULT FILE\n\n" +
			"Upload the file to the existing Amazon Glacier vault\n\n" +
			"Options:\n"

		fmt.Fprint(flag.CommandLine.Output(), usage)
		uploadCommand.PrintDefaults()

		os.Exit(2)
	}

	downloadCommand := flag.NewFlagSet("download", flag.ExitOnError)
	downloadCommand.Usage = func() {
		const usage = "Usage: surge download [options] VAULT FILE\n\n" +
			"Download an archive retrieved from the Amazon Glacier vault\n\n" +
			"Options:\n"

		fmt.Fprint(flag.CommandLine.Output(), usage)
		downloadCommand.PrintDefaults()

		os.Exit(2)
	}

	profile := flag.String("profile", "", "use a specific AWS profile")
	accountId := flag.String("account-id", "-", "the AWS account ID of the account that owns the vault")
	partSize := flag.Int64("part-size", 1048576, "the size of each part except the last, in bytes")
	jobs := flag.Int("jobs", runtime.GOMAXPROCS(0), "the maximum number of the parallel jobs")

	uploadId := uploadCommand.String("upload-id", "", "the upload ID of the multipart upload")

	jobId := downloadCommand.String("job-id", "", "the job ID whose data is downloaded (required)")

	flag.Parse()
	args := flag.Args()

	if len(args) == 0 {
		flag.Usage()
	}

	switch args[0] {
	case "download":
		downloadCommand.Parse(args[1:])
	case "upload":
		uploadCommand.Parse(args[1:])
	default:
		flag.Usage()
	}

	if downloadCommand.Parsed() {
		if *jobId == "" {
			downloadCommand.Usage()
		}

		args = downloadCommand.Args()
		if len(args) != 2 {
			downloadCommand.Usage()
		}
	}

	if uploadCommand.Parsed() {
		args = uploadCommand.Args()
		if len(args) != 2 {
			uploadCommand.Usage()
		}
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

	if uploadCommand.Parsed() {
		input := &uploader.Input{
			AccountId: *accountId,
			PartSize:  *partSize,
			VaultName: vaultName,
			FileName:  fileName,
			UploadId:  *uploadId,
		}

		u := uploader.New(service, input)

		if err := u.Upload(*jobs); err != nil {
			log.Fatal(err.Error())
		}
	}

	if downloadCommand.Parsed() {
		input := &downloader.Input{
			AccountId: *accountId,
			PartSize:  *partSize,
			VaultName: vaultName,
			FileName:  fileName,
			JobId:     *jobId,
		}

		d := downloader.New(service, input)

		if err := d.Download(*jobs); err != nil {
			log.Fatal(err.Error())
		}
	}
}
