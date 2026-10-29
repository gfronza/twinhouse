package format

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/chtype"
)

// encoderEnd is what Flush and WriteException do to an encoder that has
// written one row.
type encoderEnd struct {
	flushed, exception bool
}

func endEncoder(t *testing.T, name, dtMode string) encoderEnd {
	t.Helper()
	var buf bytes.Buffer
	cols := []Column{{Name: "t", Type: chtype.TDateTime}}
	enc, err := NewEncoder(name, &buf, cols, Options{Settings: map[string]string{"date_time_output_format": dtMode}, Timezone: "UTC"})
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.WriteRows([][]any{{time.Unix(86400, 0).UTC()}}); err != nil {
		t.Fatal(err)
	}
	if err := Flush(enc); err != nil {
		t.Fatal(err)
	}
	var end encoderEnd
	end.flushed = buf.Len() > 0
	n := buf.Len()
	ok, err := WriteException(enc, "boom")
	if err != nil {
		t.Fatal(err)
	}
	end.exception = ok && strings.Contains(buf.String()[n:], "boom")
	return end
}

// date_time_output_format wraps the encoder of a result with a DateTime
// column; Flush and WriteException reach the encoder under it.
func TestFlushAndExceptionSeeThroughDateTimeOutput(t *testing.T) {
	want := map[string]encoderEnd{
		fTabSeparated: {flushed: true}, fCSV: {flushed: true}, fValues: {flushed: true},
		fJSON: {true, true}, fJSONCompact: {true, true}, fJSONEachRow: {true, true}, fJSONCompactStringsEachRow: {true, true},
		fPretty: {}, fNative: {flushed: true},
	}
	for _, f := range formats {
		if !f.output || f.name == fNull {
			continue
		}
		simple := endEncoder(t, f.name, "simple")
		if w, ok := want[f.name]; ok && simple != w {
			t.Errorf("%s: %+v, want %+v", f.name, simple, w)
		}
		for _, mode := range []string{"iso", "unix_timestamp"} {
			if got := endEncoder(t, f.name, mode); got != simple {
				t.Errorf("%s with %s: %+v, simple %+v", f.name, mode, got, simple)
			}
		}
	}
}
