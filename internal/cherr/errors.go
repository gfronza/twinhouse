// Package cherr defines ClickHouse-compatible errors.
//
// Every error that reaches a client should be a *Error so the HTTP layer can
// render it the way ClickHouse does:
//
//	Code: 60. DB::Exception: Table db.t does not exist. (UNKNOWN_TABLE) (version 25.8.1.1)
//
// together with the X-ClickHouse-Exception-Code header and a matching HTTP status.
package cherr

import (
	"errors"
	"fmt"
	"net/http"
)

// ClickHouse error codes we emit. Values match ClickHouse's ErrorCodes.cpp;
// every code has its symbolic name in names (names.go), generated from
// ClickHouse 25.8.
const (
	CannotParseText                  = 6
	ThereIsNoColumn                  = 8
	DuplicateColumn                  = 15
	NoSuchColumnInTable              = 16
	CannotParseEscapeSequence        = 25
	CannotParseQuotedString          = 26
	CannotParseInputAssertionFailed  = 27
	AttemptToReadAfterEOF            = 32
	CannotReadAllData                = 33
	BadArguments                     = 36
	CannotParseDate                  = 38
	ChecksumDoesntMatch              = 40
	CannotParseDateTime              = 41
	NumberOfArgsDoesntMatch          = 42
	IllegalTypeOfArgument            = 43
	IllegalColumn                    = 44
	UnknownFunction                  = 46
	UnknownIdentifier                = 47
	NotImplemented                   = 48
	LogicalError                     = 49
	UnknownType                      = 50
	TypeMismatch                     = 53
	UnknownStorage                   = 56
	TableAlreadyExists               = 57
	UnknownTable                     = 60
	SyntaxError                      = 62
	UnknownAggregateFunction         = 63
	ArgumentOutOfBound               = 69
	CannotConvertType                = 70
	CannotParseNumber                = 72
	UnknownFormat                    = 73
	IncorrectQuery                   = 80
	UnknownDatabase                  = 81
	DatabaseAlreadyExists            = 82
	UnknownCompressionMethod         = 89
	EmptyDataPassed                  = 92
	UnknownSetting                   = 115
	IncorrectData                    = 117
	IncorrectResultOfScalarSubquery  = 125
	TooLargeArraySize                = 128
	CannotReadArrayFromText          = 130
	TooLargeStringSize               = 131
	IllegalDivision                  = 153
	TooManyRows                      = 158
	Timeout                          = 159 // TIMEOUT_EXCEEDED
	ReadonlyQuery                    = 164 // READONLY
	TooDeepAST                       = 167
	TooBigAST                        = 168
	IllegalAggregation               = 184
	SizesOfArraysDontMatch           = 190
	RequiredPassword                 = 194
	AmbiguousIdentifier              = 207
	NotAnAggregate                   = 215
	UnexpectedASTStructure           = 223
	QueryIsTooLarge                  = 229
	MemoryLimitExceeded              = 241
	UnionAllResultStructuresMismatch = 258
	CannotDecompress                 = 271
	TooDeepRecursion                 = 306
	CannotInsertNullInOrdinaryColumn = 349
	ZlibInflateFailed                = 354
	SessionNotFound                  = 372
	SessionIsLocked                  = 373
	InvalidSessionTimeout            = 374
	CannotParseUUID                  = 376
	DataTypeCannotHaveArguments      = 378
	HTTPLengthRequired               = 381
	NoCommonType                     = 386
	QueryWasCancelled                = 394
	TooManyRowsOrBytes               = 396
	DecimalOverflow                  = 407
	CannotCompileRegexp              = 427
	BrotliReadFailed                 = 448
	SettingConstraintViolation       = 452
	UnknownQueryParameter            = 456
	BadQueryParameter                = 457
	CannotParseBool                  = 467
	AuthenticationFailed             = 516
	ExpectedAllOrDistinct            = 558
	ZstdDecoderFailed                = 561
	Bzip2StreamDecoderFailed         = 594
	LZ4DecoderFailed                 = 618
	UnknownElementOfEnum             = 691
	SessionIDEmpty                   = 744
	Unavailable                      = 999 // KEEPER_EXCEPTION reused for "backend unavailable"
	StdException                     = 1001
)

// Name returns the symbolic ClickHouse name of code, e.g. UNKNOWN_TABLE, or
// UNKNOWN_EXCEPTION for a code ClickHouse does not define.
func Name(code int) string {
	if n, ok := names[code]; ok {
		return n
	}
	return "UNKNOWN_EXCEPTION"
}

// HTTPStatus returns the HTTP status ClickHouse 25.8 answers with for an
// exception with this code (exceptionCodeToHTTPStatus in HTTPHandler.cpp).
// The table was verified for every defined code against a ClickHouse 25.8
// server (SELECT throwIf(1, 'x', code) with
// allow_custom_error_code_in_throwif=1); every code not listed maps to 500.
func HTTPStatus(code int) int {
	switch code {
	case RequiredPassword:
		return http.StatusUnauthorized
	case 192, 193, AuthenticationFailed, 512: // UNKNOWN_USER, WRONG_PASSWORD, ..., SET_NON_GRANTED_ROLE
		return http.StatusForbidden
	case CannotParseText, ThereIsNoColumn, DuplicateColumn, CannotParseEscapeSequence, CannotParseQuotedString,
		CannotParseInputAssertionFailed, BadArguments, 37, CannotParseDate, CannotParseDateTime, IllegalColumn,
		TypeMismatch, SyntaxError, CannotParseNumber, IncorrectData, 123, 167, 168, UnexpectedASTStructure, 321,
		CannotParseUUID, CannotCompileRegexp, 441, 675, 676:
		return http.StatusBadRequest
	case UnknownFunction, UnknownIdentifier, UnknownType, UnknownStorage, UnknownTable, UnknownAggregateFunction,
		UnknownFormat, 78, UnknownDatabase, UnknownSetting, 152, 336, 511:
		return http.StatusNotFound
	case QueryIsTooLarge:
		return http.StatusRequestEntityTooLarge
	case NotImplemented:
		return http.StatusNotImplemented
	case 76, 209, 439: // CANNOT_OPEN_FILE, SOCKET_TIMEOUT, CANNOT_SCHEDULE_TASK
		return http.StatusServiceUnavailable
	case HTTPLengthRequired:
		return http.StatusLengthRequired
	case Timeout:
		return http.StatusRequestTimeout
	}
	return http.StatusInternalServerError
}

// Error is a ClickHouse-style exception.
type Error struct {
	Code  int
	Msg   string
	Cause error // optional underlying error (not shown to clients verbatim beyond Msg)
}

func (e *Error) Error() string {
	return fmt.Sprintf("Code: %d. DB::Exception: %s. (%s)", e.Code, e.Msg, e.Name())
}

func (e *Error) Unwrap() error { return e.Cause }

// Name returns the symbolic ClickHouse name, e.g. UNKNOWN_TABLE.
func (e *Error) Name() string { return Name(e.Code) }

// HTTPStatus is HTTPStatus(e.Code).
func (e *Error) HTTPStatus() int { return HTTPStatus(e.Code) }

// New builds an *Error with a formatted message.
func New(code int, format string, args ...any) *Error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, args...)}
}

// Wrap builds an *Error that keeps cause for errors.Is/As.
func Wrap(code int, cause error, format string, args ...any) *Error {
	msg := fmt.Sprintf(format, args...)
	if cause != nil {
		msg = msg + ": " + cause.Error()
	}
	return &Error{Code: code, Msg: msg, Cause: cause}
}

// As returns err as *Error (nil for nil); any other error becomes a generic
// STD_EXCEPTION (1001), not a LOGICAL_ERROR.
func As(err error) *Error {
	if err == nil {
		return nil
	}
	var ce *Error
	if errors.As(err, &ce) {
		return ce
	}
	return &Error{Code: StdException, Msg: err.Error(), Cause: err}
}

// Is reports whether err is a *Error with the given code.
func Is(err error, code int) bool {
	var ce *Error
	return errors.As(err, &ce) && ce.Code == code
}
