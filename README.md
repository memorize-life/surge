<p align="center"><img src="logo.jpg"></p>
<h6 align="center">
    Image courtesy of <a href="http://irinaklimina.com">Irina Klimina</a>
</h6>

# Surge

[![CircleCI](https://circleci.com/gh/31z4/surge.svg?style=shield&circle-token=e3e6511f388b7a7e987a596fc6a10c1b009c1efe)](https://circleci.com/gh/31z4/surge)
[![codecov](https://codecov.io/gh/31z4/surge/branch/master/graph/badge.svg)](https://codecov.io/gh/31z4/surge)
[![Go Report Card](https://goreportcard.com/badge/github.com/31z4/surge)](https://goreportcard.com/report/github.com/31z4/surge)

`surge` is a fast and reliable Amazon Glacier multipart uploader.

> Amazon Glacier is a secure, durable, and extremely low-cost cloud storage service for data archiving and long-term backup.

For more information about Glacier, see its [official documentation](https://aws.amazon.com/documentation/glacier/).

## Installation

Since `surge` is under active development you can only install it via `go get`:

    go get -u github.com/31z4/surge

## Usage

```console
$ surge -h
Usage: surge [options] VAULT FILE

Upload the file to the existing Amazon Glacier vault in multiple parts

Options:
  -account-id string
    	the AWS account ID of the account that owns the vault (default "-")
  -jobs int
    	the maximum number of the parallel uploads (default 8)
  -part-size int
    	the size of each part except the last, in bytes (default 1048576)
  -profile string
    	use a specific AWS profile
  -upload-id string
    	the upload ID of the multipart upload
```

### Create a vault

First, you need to create a Glacier vault where you're going to upload an archive.

```console
$ aws --profile glacier --region eu-central-1 glacier create-vault --account-id - --vault-name my-vault
{
    "location": "/111111111111/vaults/my-vault"
}
```

### Upload an archive

Suppose you want to upload a file called `my-archive` to `my-vault`.

```console
$ surge -profile glacier my-vault my-archive
2018/04/15 20:19:45 upload ebTlzc3QyIxUY0SjJ_p2z3QnBNDU90JWGy8EiLtnUqrHgsK3ujFyA9psn3Eg04p_s4cDsy4IR_J3g_hQuOXugMFQVA9P initiated
2018/04/15 20:19:45 start checking uploaded parts
2018/04/15 20:19:45 finish checking uploaded parts
2018/04/15 20:19:45 start uploading part (0-1048575)
2018/04/15 20:19:45 start uploading part (1048576-2097151)
2018/04/15 20:19:45 start uploading part (2097152-2621439)
2018/04/15 20:19:50 finish uploading part (2097152-2621439)
2018/04/15 20:19:52 finish uploading part (1048576-2097151)
2018/04/15 20:19:52 finish uploading part (0-1048575)
2018/04/15 20:19:53 upload location is /111111111111/vaults/my-vault/archives/KcTmz--aiKYey0dlXzVtTLfsE3TGTNB_ZHR_09NYEMCCuSWblCo4usApoAJ8hhc6qZ9ftFSDOF5KL8cHjHtohnpsSVncD6Lu58E8MKsFxZQ_TM65MIcznFJd7rEUYX0xfcqSZHRiGg
```
Make sure you save the upload location somewhere, so that you can download the archive later.

If you do not specify the `-upload-id` option, `surge` initiates a new upload and outputs its ID.

### Resume an upload

If an upload was interrupted due to a network error or any other reason you can resume it given that you have the upload ID.

```console
$ surge -profile glacier -upload-id 42-R5PIVTdOEcoDLyoRZvn6FpccADD6Wkq1o5QmQX-bDW3i_xy2kD-vTE5viY9achbKQ2yF8R27b-91TXCIZOV7w3CxR my-vault my-archive
2018/04/15 20:31:05 upload 42-R5PIVTdOEcoDLyoRZvn6FpccADD6Wkq1o5QmQX-bDW3i_xy2kD-vTE5viY9achbKQ2yF8R27b-91TXCIZOV7w3CxR initiated
2018/04/15 20:31:05 start checking uploaded parts
2018/04/15 20:31:05 part (0-1048575) is ok
2018/04/15 20:31:05 part (2097152-2621439) is ok
2018/04/15 20:31:05 finish checking uploaded parts
2018/04/15 20:31:05 start uploading part (1048576-2097151)
2018/04/15 20:31:09 finish uploading part (1048576-2097151)
2018/04/15 20:31:09 upload location is /111111111111/vaults/my-vault/archives/RTj3kf4ohj18m7poG7MEIG-zf0gRzuarPzfCKKDQWhNHELln4nV4xE7-tzHq918PIvBx8k1aLFeJ7tnZv1fLCYKqNeXi5WRpef9jcsDuFv4zEeBR4YULcT579f2Ls-WSPlhmc_R6ZQ
```
Upon that process `surge` will check for already uploaded parts and will only upload what's not uploaded or changed.

## Contributing

Contributions are greatly appreciated. The project follows the typical GitHub pull request model. Before starting any work, please either comment on an existing issue or file a new one.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.