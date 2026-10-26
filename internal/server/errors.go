package server

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// toCHError converts any error into a *cherr.Error. Context cancellation
// becomes QUERY_WAS_CANCELLED, deadline expiry TIMEOUT_EXCEEDED.
func toCHError(err error) *cherr.Error {
	var ce *cherr.Error
	if errors.As(err, &ce) {
		return ce
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return cherr.Wrap(cherr.Timeout, err, "Timeout exceeded")
	case errors.Is(err, context.Canceled):
		return cherr.Wrap(cherr.QueryWasCancelled, err, "Query was cancelled")
	}
	return cherr.As(err)
}

// exceptionText renders e exactly like ClickHouse 25.8 does in HTTP bodies
// (without the trailing newline):
//
//	Code: 60. DB::Exception: Table default.t does not exist. (UNKNOWN_TABLE) (version 25.8.1.1)
//
// ClickHouse appends '.' to the message only when it does not already end
// with one.
func exceptionText(e *cherr.Error, version string) string {
	msg := e.Msg
	if !strings.HasSuffix(msg, ".") {
		msg += "."
	}
	return fmt.Sprintf("Code: %d. DB::Exception: %s (%s) (version %s)", e.Code, msg, e.Name(), version)
}

// jsonExceptionFormats are the output formats in which ClickHouse writes an
// exception inside the format itself (http_write_exception_in_output_format)
// instead of as plain text.
var jsonExceptionFormats = map[string]byte{
	"JSON":                                       'j',
	"JSONStrings":                                'j',
	"JSONCompact":                                'j',
	"JSONCompactStrings":                         'j',
	"JSONEachRow":                                'o',
	"JSONStringsEachRow":                         'o',
	"JSONCompactEachRow":                         'a',
	"JSONCompactStringsEachRow":                  'a',
	"JSONCompactEachRowWithNames":                '2',
	"JSONCompactStringsEachRowWithNames":         '2',
	"JSONCompactEachRowWithNamesAndTypes":        '3',
	"JSONCompactStringsEachRowWithNamesAndTypes": '3',
}

// formatException renders an exception that happened before any result data
// was sent, in the output format fmtName when ClickHouse does so for that
// format. ok is false when the format renders exceptions as plain text.
func formatException(fmtName, text string) (body string, ok bool) {
	kind, ok := jsonExceptionFormats[fmtName]
	if !ok {
		return "", false
	}
	q := string(appendJSONString(nil, text))
	switch kind {
	case 'j':
		return "{\n\t\"meta\":\n\t[\n\n\t],\n\n\t\"data\":\n\t[\n\n\t],\n\n\t\"rows\": 0,\n\n\t\"exception\": " + q + "\n}\n", true
	case 'o':
		return "{\"exception\": " + q + "}\n", true
	case 'a':
		return "[" + q + "]\n", true
	case '2':
		return "[]\n[" + q + "]\n", true
	case '3':
		return "[]\n[]\n[" + q + "]\n", true
	}
	return "", false
}

// appendJSONString appends s as a JSON string literal using ClickHouse's
// escaping rules (forward slashes are escaped, as with the default
// output_format_json_escape_forward_slashes=1; U+2028/U+2029 are escaped).
func appendJSONString(dst []byte, s string) []byte {
	const hex = "0123456789abcdef"
	dst = append(dst, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == 0xE2 && i+2 < len(s) && s[i+1] == 0x80 && (s[i+2] == 0xA8 || s[i+2] == 0xA9) {
			if s[i+2] == 0xA8 {
				dst = append(dst, `\u2028`...)
			} else {
				dst = append(dst, `\u2029`...)
			}
			i += 2
			continue
		}
		switch c {
		case '"':
			dst = append(dst, '\\', '"')
		case '\\':
			dst = append(dst, '\\', '\\')
		case '/':
			dst = append(dst, '\\', '/')
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = append(dst, '\\', 'f')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			if c < 0x20 {
				dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
			} else {
				dst = append(dst, c)
			}
		}
	}
	return append(dst, '"')
}
