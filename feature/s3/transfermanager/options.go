package transfermanager

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
)

// Options provides params needed for transfer api calls
type Options struct {
	// The client to use when uploading to S3.
	S3 S3APIClient

	// The buffer size (in bytes) to use when buffering data into chunks and
	// sending them as parts to S3. The minimum allowed part size is 5MB, and
	// if this value is set to zero, the DefaultUploadPartSize value will be used.
	PartSizeBytes int64

	// The threshold bytes to decide when the file should be multi-uploaded
	MultipartUploadThreshold int64

	// FailTimeout is the timeout for transfer failure handling when a transfer fails.
	// A fresh context with this timeout is used so failure followup (AbortMPU for upload or possible progress listener work)
	// succeed even when the original context is canceled.
	// Defaults to 0 (uses the original context) for fail case not caused by ctx cancellation.
	FailTimeout time.Duration

	// The max parts count for a multi part upload, which must not exceed "Maximum number of parts per upload" defined by S3
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
	MaxUploadParts int64

	// Option to disable checksum validation for download
	DisableChecksumValidation bool

	// Checksum algorithm to use for upload
	ChecksumAlgorithm types.ChecksumAlgorithm

	// RequestChecksumCalculation determines when request checksums are calculated
	// for uploads. This setting takes precedence over the RequestChecksumCalculation
	// configured on the underlying S3 client.
	//
	// There are two possible values:
	//
	//  1. RequestChecksumCalculationWhenSupported (default): a checksum is
	//     calculated for every upload request, defaulting to CRC32 when the caller
	//     does not specify a ChecksumAlgorithm.
	//
	//  2. RequestChecksumCalculationWhenRequired: a checksum is only calculated
	//     when the caller specifies a ChecksumAlgorithm (on the input or these
	//     options) or the operation otherwise requires it.
	//
	// Note: S3 Express One Zone (directory) buckets always require CRC32 checksums,
	// which are applied regardless of this setting.
	RequestChecksumCalculation aws.RequestChecksumCalculation

	// The number of goroutines to spin up in parallel per call to transfer single object parts or directory objects.
	// If this is set to zero, the DefaultUploadConcurrency value will be used.
	//
	// The concurrency pool is not shared between multiple API calls.
	Concurrency int

	// The type indicating if object is multi-downloaded in parts or ranges
	GetObjectType types.GetObjectType

	// PartBodyMaxRetries is the number of retry attempts to make for failed part downloads.
	PartBodyMaxRetries int

	// Max size for the GetObject memory buffer. The reader returned from GetObject can buffer up to
	// <GetObjectBufferSize> bytes of data at any time and only reads more data when user completely consumes
	// current data buffered. This mechanism avoids unbounded memory usage when downloading large object via GetObject
	GetObjectBufferSize int64

	// DirectIOThreshold is the object-size threshold in bytes above which
	// DownloadObject/DownloadFile opt a *os.File destination into O_DIRECT (Linux
	// only). Objects at or below this size use the caller's WriterAt unmodified. If
	// zero, defaultDirectIOThreshold (100 MiB) is used.
	DirectIOThreshold int64

	// DisableDirectIO forces DownloadObject/DownloadFile to leave a *os.File
	// destination unmodified regardless of object size or platform.
	DisableDirectIO bool

	// WriteChunkSizeBytes is the fixed size in bytes of each O_DIRECT write issued
	// to a *os.File destination, once DownloadObject has opted it into O_DIRECT.
	// It is rounded up to the device block size, and PartSizeBytes is in turn
	// rounded up to a multiple of it, so that no chunk-sized write region ever
	// needs bytes from two different part-workers. If zero,
	// defaultWriteChunkSizeBytes (8 MiB) is used.
	WriteChunkSizeBytes int64

	// Registry of single object progress listener hooks.
	//
	// It is safe to modify the registry in per-operation functional options,
	// the original client-level registry will not be affected.
	ObjectProgressListeners ObjectProgressListeners

	// Registry of directory progress listener hooks.
	//
	// It is safe to modify the registry in per-operation functional options,
	// the original client-level registry will not be affected.
	DirectoryProgressListeners DirectoryProgressListeners
}

func (o *Options) init() {
}

func resolveConcurrency(o *Options) {
	if o.Concurrency == 0 {
		o.Concurrency = defaultTransferConcurrency
	}
}

func resolvePartSizeBytes(o *Options) {
	if o.PartSizeBytes == 0 {
		o.PartSizeBytes = defaultPartSizeBytes
	}
}

func resolveRequestChecksumCalculation(o *Options) {
	if o.RequestChecksumCalculation == aws.RequestChecksumCalculationUnset {
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenSupported
	}
}

func (o Options) checksumAlgorithm() types.ChecksumAlgorithm {
	if o.ChecksumAlgorithm != "" {
		return o.ChecksumAlgorithm
	}

	if o.RequestChecksumCalculation != aws.RequestChecksumCalculationWhenRequired {
		return types.ChecksumAlgorithmCrc32
	}

	// Empty: the RequestChecksumCalculation client option applied to each S3 call
	// suppresses the client's own default checksum.
	return ""
}

func resolveMultipartUploadThreshold(o *Options) {
	if o.MultipartUploadThreshold == 0 {
		o.MultipartUploadThreshold = defaultMultipartUploadThreshold
	}
}

func resolveGetObjectType(o *Options) {
	if o.GetObjectType == "" {
		o.GetObjectType = types.GetObjectParts
	}
}

func resolvePartBodyMaxRetries(o *Options) {
	if o.PartBodyMaxRetries == 0 {
		o.PartBodyMaxRetries = defaultPartBodyMaxRetries
	}
}

func resolveGetBufferSize(o *Options) {
	if o.GetObjectBufferSize == 0 {
		o.GetObjectBufferSize = defaultGetBufferSize
	}
}

func resolveMaxUploadParts(o *Options) {
	if o.MaxUploadParts == 0 {
		o.MaxUploadParts = defaultMaxUploadParts
	}
}

func resolveDirectIOThreshold(o *Options) {
	if o.DirectIOThreshold == 0 {
		o.DirectIOThreshold = defaultDirectIOThreshold
	}
}

func resolveWriteChunkSizeBytes(o *Options) {
	if o.WriteChunkSizeBytes == 0 {
		o.WriteChunkSizeBytes = defaultWriteChunkSizeBytes
	}
}

// Copy returns new copy of the Options
func (o Options) Copy() Options {
	to := o
	to.ObjectProgressListeners = to.ObjectProgressListeners.Copy()
	return to
}
