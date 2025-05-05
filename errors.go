package dedup

import "github.com/pixie-sh/errors-go"

var (
	DedupErrorCodeNumber    = 100000
	DedupEntityNilErrorCode = errors.NewErrorCode("DedupEntityNilErrorCode", DedupErrorCodeNumber+errors.HTTPBadRequest)
	DedupInvalidHashErrorCode = errors.NewErrorCode("DedupInvalidHashErrorCode", DedupErrorCodeNumber+errors.HTTPServerError)
	DedupStorageErrorCode = errors.NewErrorCode("DedupStorageErrorCode", DedupErrorCodeNumber+errors.HTTPServerError)
	DedupMissingKeyErrorCode = errors.NewErrorCode("DedupMissingKeyErrorCode", DedupErrorCodeNumber+errors.HTTPServerError)
	DedupNoExpeirationKeyErrorCode = errors.NewErrorCode("DedupNoExpeirationKeyErrorCode", DedupErrorCodeNumber+errors.HTTPServerError)
	DedupEntityTypeMismatchErrorCode = errors.NewErrorCode("DedupEntityTypeMismatchErrorCode", DedupErrorCodeNumber+errors.HTTPBadRequest)
)