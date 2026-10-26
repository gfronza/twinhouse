package format

import (
	"bufio"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/gfronza/twinhouse/internal/chtype"
)

// prettyEncoder implements Pretty and PrettyCompact (best effort: the layout
// matches ClickHouse for common results; ClickHouse's colour, digit-group
// highlighting and vertical fallback are not reproduced). It buffers at most
// output_format_pretty_max_rows rows: like ClickHouse it shows the first and
// last halves of larger results.
type prettyEncoder struct {
	w          *bufio.Writer
	cols       []Column
	types      []chtype.Type
	compact    bool
	rowNumbers bool
	maxRows    int
	maxWidth   int
	footerMin  int
	footer     bool
	vertical   bool // output_format_pretty_fallback_to_vertical
	vMaxRows   int
	vMinCols   int
	vMinWidth  int

	head  []prettyRow
	tail  []prettyRow // ring buffer of the last rows once head is full
	tailN int         // next write position in tail
	total int
}

type prettyRow struct {
	num   int
	cells [][]string // per column: lines
}

func newPrettyEncoder(name string, w io.Writer, cols []Column, opts Options) *prettyEncoder {
	s := opts.Settings
	e := &prettyEncoder{
		w:          bufio.NewWriterSize(w, outBufSize),
		cols:       cols,
		types:      renderTypes(cols, opts),
		compact:    name == fPrettyCompact,
		rowNumbers: settingBool(s, "output_format_pretty_row_numbers", true),
		maxRows:    int(settingInt(s, "output_format_pretty_max_rows", 1000)),
		maxWidth:   int(settingInt(s, "output_format_pretty_max_value_width", 10000)),
		footerMin:  int(settingInt(s, "output_format_pretty_display_footer_column_names_min_rows", 50)),
		footer:     settingBool(s, "output_format_pretty_display_footer_column_names", true),
		vertical:   settingBool(s, "output_format_pretty_fallback_to_vertical", true),
		vMaxRows:   int(settingInt(s, "output_format_pretty_fallback_to_vertical_max_rows_per_chunk", 10)),
		vMinCols:   int(settingInt(s, "output_format_pretty_fallback_to_vertical_min_columns", 5)),
		vMinWidth:  int(settingInt(s, "output_format_pretty_fallback_to_vertical_min_table_width", 250)),
	}
	if e.maxRows <= 0 {
		e.maxRows = 1 << 62
	}
	return e
}

func (e *prettyEncoder) cell(i int, v any) []string {
	var s string
	if v == nil {
		s = nullText
	} else {
		s = rawText(e.types[i], v)
	}
	lines := strings.Split(s, "\n")
	for j, l := range lines {
		if e.maxWidth > 0 && utf8.RuneCountInString(l) > e.maxWidth {
			r := []rune(l)
			lines[j] = string(r[:e.maxWidth]) + "⋯"
		}
	}
	return lines
}

func (e *prettyEncoder) WriteRows(rows [][]any) error {
	if err := checkRowWidth(rows, len(e.cols)); err != nil {
		return err
	}
	headCap := e.maxRows - e.maxRows/2
	tailCap := e.maxRows / 2
	for _, r := range rows {
		e.total++
		pr := prettyRow{num: e.total, cells: make([][]string, len(r))}
		for i, v := range r {
			pr.cells[i] = e.cell(i, v)
		}
		switch {
		case len(e.head) < headCap:
			e.head = append(e.head, pr)
		case tailCap == 0:
		case len(e.tail) < tailCap:
			e.tail = append(e.tail, pr)
		default:
			e.tail[e.tailN] = pr
			e.tailN = (e.tailN + 1) % tailCap
		}
	}
	return nil
}

func alignRight(t chtype.Type) bool {
	switch baseKind(t) {
	case chtype.UInt8, chtype.UInt16, chtype.UInt32, chtype.UInt64,
		chtype.Int8, chtype.Int16, chtype.Int32, chtype.Int64,
		chtype.Float32, chtype.Float64,
		chtype.Date, chtype.Date32, chtype.DateTime, chtype.DateTime64:
		return true
	}
	return false
}

func runeLen(s string) int { return utf8.RuneCountInString(s) }

func (e *prettyEncoder) Finish(Stats) error {
	if e.total == 0 {
		return e.w.Flush()
	}
	tail := append(append([]prettyRow(nil), e.tail[e.tailN:]...), e.tail[:e.tailN]...)
	rows := append(append([]prettyRow(nil), e.head...), tail...)
	gapAfter := -1
	if e.total > len(rows) {
		gapAfter = len(e.head)
	}

	widths := make([]int, len(e.cols))
	right := make([]bool, len(e.cols))
	for i, c := range e.cols {
		widths[i] = runeLen(c.Name)
		right[i] = alignRight(c.Type)
	}
	for _, r := range rows {
		for i, lines := range r.cells {
			for _, l := range lines {
				widths[i] = max(widths[i], runeLen(l))
			}
		}
	}
	if e.useVertical(rows, widths) {
		return e.writeVertical(rows)
	}
	numW := 0
	if e.rowNumbers {
		numW = len(strconv.Itoa(rows[len(rows)-1].num)) + 2
	}
	blank := strings.Repeat(" ", numW)

	pad := func(s string, w int, right bool) string {
		n := w - runeLen(s)
		if n <= 0 {
			return s
		}
		if right {
			return strings.Repeat(" ", n) + s
		}
		return s + strings.Repeat(" ", n)
	}
	// rule draws a horizontal line; with names, the names are embedded.
	rule := func(left, mid, rightC, fill string, names bool) {
		e.w.WriteString(blank)
		e.w.WriteString(left)
		for i, c := range e.cols {
			if i > 0 {
				e.w.WriteString(mid)
			}
			if names {
				e.w.WriteString(fill)
				n := widths[i] - runeLen(c.Name)
				if right[i] {
					e.w.WriteString(strings.Repeat(fill, n))
					e.w.WriteString(c.Name)
				} else {
					e.w.WriteString(c.Name)
					e.w.WriteString(strings.Repeat(fill, n))
				}
				e.w.WriteString(fill)
			} else {
				e.w.WriteString(strings.Repeat(fill, widths[i]+2))
			}
		}
		e.w.WriteString(rightC)
		e.w.WriteByte('\n')
	}
	nameRow := func(sep string) {
		e.w.WriteString(blank)
		for i, c := range e.cols {
			e.w.WriteString(sep)
			e.w.WriteByte(' ')
			e.w.WriteString(pad(c.Name, widths[i], right[i]))
			e.w.WriteByte(' ')
		}
		e.w.WriteString(sep)
		e.w.WriteByte('\n')
	}

	if e.compact {
		rule("┌", "┬", "┐", "─", true)
	} else {
		rule("┏", "┳", "┓", "━", false)
		nameRow("┃")
		rule("┡", "╇", "┩", "━", false)
	}
	for ri, r := range rows {
		if ri > 0 && !e.compact && ri != gapAfter {
			rule("├", "┼", "┤", "─", false)
		}
		if ri == gapAfter {
			e.w.WriteString(blank)
			e.w.WriteString("─ ─ ─ ─ ─ \n")
		}
		height := 1
		for _, lines := range r.cells {
			height = max(height, len(lines))
		}
		for li := 0; li < height; li++ {
			if li == 0 && e.rowNumbers {
				e.w.WriteString(pad(strconv.Itoa(r.num)+". ", numW, true))
			} else {
				e.w.WriteString(blank)
			}
			for i, lines := range r.cells {
				e.w.WriteString("│")
				if li >= len(lines) {
					e.w.WriteString(strings.Repeat(" ", widths[i]+2))
					continue
				}
				lead, trail := " ", " "
				if li > 0 {
					lead = "↳"
				}
				if li < len(lines)-1 {
					trail = "↴"
				}
				e.w.WriteString(lead)
				e.w.WriteString(pad(lines[li], widths[i], right[i] && len(lines) == 1))
				e.w.WriteString(trail)
			}
			e.w.WriteString("│\n")
		}
	}
	withFooter := e.footer && e.total >= e.footerMin
	switch {
	case e.compact:
		rule("└", "┴", "┘", "─", withFooter)
	case withFooter:
		rule("┢", "╈", "┪", "━", false)
		nameRow("┃")
		rule("┗", "┻", "┛", "━", false)
	default:
		rule("└", "┴", "┘", "─", false)
	}
	if e.total >= e.maxRows {
		e.w.WriteString("Showed " + strconv.Itoa(len(rows)) + " out of " + strconv.Itoa(e.total) + " rows.\n")
	}
	return e.w.Flush()
}

// useVertical mirrors ClickHouse's fallback to the Vertical layout for short
// results that are wide (sum of column widths) or contain multi-line values.
func (e *prettyEncoder) useVertical(rows []prettyRow, widths []int) bool {
	if !e.vertical || len(rows) > e.vMaxRows || len(e.cols) < e.vMinCols {
		return false
	}
	total := 0
	for _, w := range widths {
		total += w
	}
	if total >= e.vMinWidth {
		return true
	}
	for _, r := range rows {
		for _, lines := range r.cells {
			if len(lines) > 1 {
				return true
			}
		}
	}
	return false
}

func (e *prettyEncoder) writeVertical(rows []prettyRow) error {
	nameW := 0
	for _, c := range e.cols {
		nameW = max(nameW, runeLen(c.Name))
	}
	for ri, r := range rows {
		if ri > 0 {
			e.w.WriteByte('\n')
		}
		title := "Row " + strconv.Itoa(r.num) + ":"
		e.w.WriteString(title + "\n")
		e.w.WriteString(strings.Repeat("─", runeLen(title)) + "\n")
		for i, c := range e.cols {
			e.w.WriteString(c.Name)
			e.w.WriteString(":")
			e.w.WriteString(strings.Repeat(" ", nameW-runeLen(c.Name)+1))
			e.w.WriteString(strings.Join(r.cells[i], "\n"))
			e.w.WriteByte('\n')
		}
	}
	return e.w.Flush()
}
