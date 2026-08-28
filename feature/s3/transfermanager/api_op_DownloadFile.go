package transfermanager

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// DownloadFileInput represents a request to the DownloadFile() call. It mirrors the
// common fields of an S3 GetObject request, but instead of a caller-supplied
// io.WriterAt the object is written to a local file at FilePath. The transfer
// manager owns the destination writer, which lets it pick an O_DIRECT or buffered
// strategy based on object size and write in fixed-size chunks.
type DownloadFileInput struct {
	// Bucket where the object is downloaded from.
	Bucket *string

	// Key of the object to get.
	Key *string

	// FilePath is the local destination path the object is written to. The file is
	// created (or truncated if it exists). Required.
	FilePath string

	// To retrieve the checksum, this mode must be enabled.
	ChecksumMode types.ChecksumMode

	// The account ID of the expected bucket owner.
	ExpectedBucketOwner *string

	// Return the object only if its entity tag (ETag) is the same as the one
	// specified; otherwise, return a 412 Precondition Failed error.
	IfMatch *string

	// Return the object only if it has been modified since the specified time;
	// otherwise, return a 304 Not Modified error.
	IfModifiedSince *time.Time

	// Return the object only if its entity tag (ETag) is different from the one
	// specified; otherwise, return a 304 Not Modified error.
	IfNoneMatch *string

	// Return the object only if it has not been modified since the specified time;
	// otherwise, return a 412 Precondition Failed error.
	IfUnmodifiedSince *time.Time

	// Downloads the specified byte range of an object. Only applies when
	// GetObjectType is GetObjectRanges.
	Range *string

	// Confirms that the requester knows that they will be charged for the request.
	RequestPayer types.RequestPayer

	// Sets the Cache-Control header of the response.
	ResponseCacheControl *string

	// Sets the Content-Disposition header of the response.
	ResponseContentDisposition *string

	// Sets the Content-Encoding header of the response.
	ResponseContentEncoding *string

	// Sets the Content-Language header of the response.
	ResponseContentLanguage *string

	// Sets the Content-Type header of the response.
	ResponseContentType *string

	// Sets the Expires header of the response.
	ResponseExpires *time.Time

	// Specifies the algorithm to use when decrypting the object (for example, AES256).
	SSECustomerAlgorithm *string

	// Specifies the customer-provided encryption key for Amazon S3 to decrypt the
	// object.
	SSECustomerKey *string

	// Specifies the 128-bit MD5 digest of the customer-provided encryption key.
	SSECustomerKeyMD5 *string

	// Version ID used to reference a specific version of the object.
	VersionID *string
}

// toDownloadObjectInput builds the DownloadObjectInput the shared downloader
// consumes, wiring the internal file sink as the destination WriterAt.
func (i *DownloadFileInput) toDownloadObjectInput(w fileSink) *DownloadObjectInput {
	return &DownloadObjectInput{
		Bucket:                     i.Bucket,
		Key:                        i.Key,
		WriterAt:                   w,
		ChecksumMode:               i.ChecksumMode,
		ExpectedBucketOwner:        i.ExpectedBucketOwner,
		IfMatch:                    i.IfMatch,
		IfModifiedSince:            i.IfModifiedSince,
		IfNoneMatch:                i.IfNoneMatch,
		IfUnmodifiedSince:          i.IfUnmodifiedSince,
		Range:                      i.Range,
		RequestPayer:               i.RequestPayer,
		ResponseCacheControl:       i.ResponseCacheControl,
		ResponseContentDisposition: i.ResponseContentDisposition,
		ResponseContentEncoding:    i.ResponseContentEncoding,
		ResponseContentLanguage:    i.ResponseContentLanguage,
		ResponseContentType:        i.ResponseContentType,
		ResponseExpires:            i.ResponseExpires,
		SSECustomerAlgorithm:       i.SSECustomerAlgorithm,
		SSECustomerKey:             i.SSECustomerKey,
		SSECustomerKeyMD5:          i.SSECustomerKeyMD5,
		VersionID:                  i.VersionID,
	}
}

// mapHeadObjectInput builds the HeadObject request used to size the object before
// choosing the destination writer strategy.
func (i *DownloadFileInput) mapHeadObjectInput() *s3.HeadObjectInput {
	return &s3.HeadObjectInput{
		Bucket:               i.Bucket,
		Key:                  i.Key,
		ExpectedBucketOwner:  i.ExpectedBucketOwner,
		IfMatch:              i.IfMatch,
		IfModifiedSince:      i.IfModifiedSince,
		IfNoneMatch:          i.IfNoneMatch,
		IfUnmodifiedSince:    i.IfUnmodifiedSince,
		Range:                i.Range,
		RequestPayer:         s3types.RequestPayer(i.RequestPayer),
		SSECustomerAlgorithm: i.SSECustomerAlgorithm,
		SSECustomerKey:       i.SSECustomerKey,
		SSECustomerKeyMD5:    i.SSECustomerKeyMD5,
		VersionId:            i.VersionID,
	}
}

// DownloadFile downloads an object from S3 to a local file at input.FilePath,
// splitting large objects into parts/ranges fetched in parallel (the same engine
// as DownloadObject). Unlike DownloadObject, the transfer manager owns the
// destination writer: objects larger than Options.DirectIOThreshold (default
// 100 MiB) are written with O_DIRECT on Linux, smaller objects use a buffered
// writer, and in both cases writes are coalesced into fixed Options.WriteChunkSizeBytes
// chunks (default 8 MiB) regardless of the download range/part size.
//
// DownloadFile defaults to range-based GETs (GetObjectRanges) with an 8 MiB part
// size; callers can override GetObjectType, PartSizeBytes, Concurrency, and the
// other download knobs via functional options, exactly as with DownloadObject.
//
// The returned DownloadObjectOutput carries the object metadata (the response Body
// is replaced by the on-disk file).
func (c *Client) DownloadFile(ctx context.Context, input *DownloadFileInput, opts ...func(*Options)) (*DownloadObjectOutput, error) {
	if input == nil || input.FilePath == "" {
		return nil, fmt.Errorf("DownloadFile: FilePath is required")
	}

	options := c.options.Copy()
	// DownloadFile defaults to range GETs; the client-level default is PART. Applying
	// this before the per-call opts lets callers override it back to parts.
	options.GetObjectType = types.GetObjectRanges
	for _, opt := range opts {
		opt(&options)
	}
	// Defensively re-resolve in case a functional option zeroed a value.
	resolveConcurrency(&options)
	resolvePartSizeBytes(&options)
	resolvePartBodyMaxRetries(&options)
	resolveDirectIOThreshold(&options)
	resolveWriteChunkSizeBytes(&options)

	size, err := c.downloadFileObjectSize(ctx, input, &options)
	if err != nil {
		return nil, err
	}

	sink, err := newFileSink(input.FilePath, size, &options)
	if err != nil {
		return nil, fmt.Errorf("DownloadFile: open destination %q: %w", input.FilePath, err)
	}

	d := downloader{in: input.toDownloadObjectInput(sink), options: options}
	out, derr := d.download(ctx)

	// Always Close the sink to flush the trailing chunk and finalize/close the file,
	// even on download error (to release the fd).
	cerr := sink.Close()
	if derr != nil {
		return out, derr
	}
	if cerr != nil {
		return out, fmt.Errorf("DownloadFile: finalize destination %q: %w", input.FilePath, cerr)
	}
	return out, nil
}

// downloadFileObjectSize determines the size used to choose the destination writer
// strategy. For a ranged request the destination is only the range length; otherwise
// it issues a HeadObject to read ContentLength.
func (c *Client) downloadFileObjectSize(ctx context.Context, input *DownloadFileInput, o *Options) (int64, error) {
	if r := aws.ToString(input.Range); r != "" {
		if start, end, err := getReqRange(r); err == nil && end >= start {
			return end - start + 1, nil
		}
	}
	head, err := o.S3.HeadObject(ctx, input.mapHeadObjectInput())
	if err != nil {
		return 0, fmt.Errorf("DownloadFile: HeadObject %q: %w", aws.ToString(input.Key), err)
	}
	return aws.ToInt64(head.ContentLength), nil
}
