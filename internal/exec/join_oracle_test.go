package exec

import (
	"io"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/exec/aggstate"
)

// This file keeps the join build and probe as they were before the probe
// streamed, as an oracle for TestJoinMatchesOldImplementation.

// startOldJoin is startJoin with the old implementation.
func startOldJoin(q *query, spec *joinSpec, rit rowIter, openLeft func() (rowIter, error)) (rowIter, error) {
	build, err := oldBuild(spec, q, rit)
	rit.close()
	if err != nil {
		return nil, err
	}
	lit, err := openLeft()
	if err != nil {
		return nil, err
	}
	return &oldJoinIter{spec: spec, b: build, in: lit}, nil
}

func oldKeyOf(spec *joinSpec, row []any, keys []*expr, convs []func(any) (any, error)) (string, bool, error) {
	var b []byte
	for i, k := range keys {
		v, err := k.eval(row)
		if err != nil {
			return "", false, err
		}
		if v == nil {
			return "", false, nil // NULL never matches
		}
		if v, err = convs[i](v); err != nil {
			return "", false, err
		}
		b = aggstate.AppendKey(b, v)
	}
	return string(b), true, nil
}

type oldJoinBuild struct {
	rows    [][]any // combined-width rows with the left part empty
	index   map[string][]int
	matched []bool
}

func oldBuild(spec *joinSpec, q *query, it rowIter) (*oldJoinBuild, error) {
	b := &oldJoinBuild{index: map[string][]int{}}
	for {
		r, err := it.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		row := make([]any, spec.leftW+spec.rightW)
		copy(row[spec.leftW:], r)
		k, ok, err := oldKeyOf(spec, row, spec.rightKeys, spec.keyConvR)
		if err != nil {
			return nil, err
		}
		b.rows = append(b.rows, row)
		if ok {
			b.index[k] = append(b.index[k], len(b.rows)-1)
		}
		if len(b.rows)&1023 == 0 {
			if err := q.ctx.Err(); err != nil {
				return nil, cherr.Wrap(cherr.Timeout, err, "query cancelled")
			}
		}
	}
	b.matched = make([]bool, len(b.rows))
	return b, nil
}

type oldJoinIter struct {
	spec  *joinSpec
	b     *oldJoinBuild
	in    rowIter
	buf   [][]any
	done  bool
	tailI int
}

func (j *oldJoinIter) combine(left []any, right []any) []any {
	s := j.spec
	row := make([]any, s.leftW+s.rightW)
	if left != nil {
		copy(row, left[:s.leftW])
	} else {
		copy(row, s.leftZero)
	}
	if right != nil {
		copy(row[s.leftW:], right[s.leftW:])
	} else {
		copy(row[s.leftW:], s.rightZero)
	}
	if left == nil {
		for li, ri := range s.using {
			row[li] = row[ri]
		}
	}
	if right == nil && left != nil {
		for _, f := range s.antiFill {
			if v, err := f.conv(row[f.left]); err == nil {
				row[f.right] = v
			}
		}
	}
	return row
}

func (j *oldJoinIter) next() ([]any, error) {
	s := j.spec
	for len(j.buf) == 0 {
		if j.done {
			// Unmatched right rows of RIGHT/FULL joins.
			for j.tailI < len(j.b.rows) {
				i := j.tailI
				j.tailI++
				if !j.b.matched[i] {
					return j.combine(nil, j.b.rows[i]), nil
				}
			}
			return nil, io.EOF
		}
		left, err := j.in.next()
		if err == io.EOF {
			j.done = true
			if s.kind != "RIGHT" && s.kind != "FULL" {
				return nil, io.EOF
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		var cands []int
		if len(s.leftKeys) == 0 {
			cands = make([]int, len(j.b.rows))
			for i := range cands {
				cands[i] = i
			}
		} else {
			probe := make([]any, s.leftW+s.rightW)
			copy(probe, left)
			k, ok, err := oldKeyOf(s, probe, s.leftKeys, s.keyConvL)
			if err != nil {
				return nil, err
			}
			if ok {
				cands = j.b.index[k]
			}
		}
		matched := false
		for _, ci := range cands {
			row := j.combine(left, j.b.rows[ci])
			if s.residual != nil {
				v, err := s.residual.eval(row)
				if err != nil {
					return nil, err
				}
				if v == nil {
					continue
				}
				ok, err := truth(v)
				if err != nil {
					return nil, err
				}
				if !ok {
					continue
				}
			}
			matched = true
			j.b.matched[ci] = true
			if s.strictness == "ANTI" {
				break
			}
			j.buf = append(j.buf, row)
			if s.strictness == "ANY" || s.strictness == "SEMI" {
				break
			}
		}
		if !matched && (s.kind == "LEFT" && s.strictness != "SEMI" || s.kind == "FULL") {
			j.buf = append(j.buf, j.combine(left, nil))
		}
	}
	r := j.buf[0]
	j.buf = j.buf[1:]
	return r, nil
}

func (j *oldJoinIter) close() { j.in.close() }
