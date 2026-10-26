// Package limits holds the caps that stop one query or one request body from
// exhausting the process. Go's recover() cannot catch running out of memory
// or overflowing the stack, so anything sized or nested by user input must be
// checked against one of these before it allocates or recurses.
//
// Where ClickHouse has an equivalent setting, the value matches its default.
// The rest are sized for the smallest deployment (a 1 GiB Lambda).
package limits

const (
	// MaxParserDepth bounds recursion while parsing SQL text and type
	// strings (ClickHouse max_parser_depth).
	MaxParserDepth = 1000

	// MaxASTDepth bounds the depth of a parsed expression tree
	// (ClickHouse max_ast_depth), so later recursive passes cannot
	// overflow the stack.
	MaxASTDepth = 1000

	// MaxASTElements bounds the node count of a parsed query
	// (ClickHouse max_ast_elements).
	MaxASTElements = 50_000

	// MaxExpandedASTElements bounds the node count after alias and WITH
	// expansion (ClickHouse max_expanded_ast_elements).
	MaxExpandedASTElements = 500_000

	// MaxFixedStringSize is the largest N accepted for FixedString(N)
	// (ClickHouse MAX_FIXEDSTRING_SIZE).
	MaxFixedStringSize = 0xFFFFFF

	// MaxRepeatTimes is the largest repeat count accepted by repeat()
	// (ClickHouse's limit in FunctionRepeat).
	MaxRepeatTimes = 1_000_000

	// MaxFunctionResultBytes caps the size of one string or array value
	// built by a function from its arguments.
	MaxFunctionResultBytes = 64 << 20

	// MaxRangeElements caps the elements of one range() result. ClickHouse
	// allows 500M per block; one boxed element here costs about 24 bytes,
	// so the cap keeps a single value near MaxFunctionResultBytes.
	MaxRangeElements = 2_000_000

	// MaxRegexpCacheEntries bounds the process-wide compiled-regexp cache.
	MaxRegexpCacheEntries = 1024

	// NativeAllocChunkRows is how many rows a decoder may allocate ahead
	// of the data it has actually read from an untrusted body.
	NativeAllocChunkRows = 64 << 10
)
