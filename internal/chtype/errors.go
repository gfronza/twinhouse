package chtype

import "dynamodb-eventstream/internal/cherr"

// ClickHouse error codes used by this package that internal/cherr does not
// (yet) name. Values match ClickHouse's ErrorCodes.cpp so clients that switch
// on the numeric code behave as with a real server.
const (
	codeDuplicateColumn           = 15
	codeCannotParseEscapeSequence = 25
	codeCannotParseQuotedString   = 26
	codeCannotParseInputAssertion = 27
	codeAttemptToReadAfterEOF     = 32
	codeCannotParseDate           = 38
	codeArgumentOutOfBound        = 69
	codeCannotParseNumber         = 72
	codeCannotReadArrayFromText   = 130
	codeTooLargeStringSize        = 131
	codeCannotInsertNull          = 349
	codeCannotParseUUID           = 376
	codeNoCommonType              = 386
	codeDecimalOverflow           = 407
	codeCannotParseBool           = 467
	codeUnknownElementOfEnum      = 691
)

func errf(code int, format string, args ...any) error {
	return cherr.New(code, format, args...)
}
