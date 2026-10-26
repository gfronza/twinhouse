package sqlparse

import (
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/limits"
)

// The parser builds operator and postfix chains in loops, so its recursion
// depth does not bound the height of the tree it returns. It works the
// height out while building, and stops at ClickHouse's max_ast_depth before
// later recursive passes can overflow the stack.
//
// p.height is the height of the tallest expression finished since the
// current node's children began (beginChildren). A child is finished before
// its parent, so a node is one level above the tallest expression finished
// while its children were parsed. A collection of literals is one node of
// height 1, a single literal in ClickHouse.
//
// p.elements counts nodes toward max_ast_elements. As in ClickHouse, a
// collection of literals ([1, 2], (1, 'a') and nestings of them) is one
// node, a chain of AND, OR or || is one function, an alias is not a node,
// and INSERT ... VALUES rows are data, not AST.

// beginChildren starts the children of a node and returns the state that
// endChildren restores.
func (p *parser) beginChildren() int {
	saved := p.height
	p.height = 0
	return saved
}

// endChildren returns the height of the tallest child finished since
// beginChildren returned saved.
func (p *parser) endChildren(saved int) int {
	h := p.height
	p.height = saved
	return h
}

// finished records an expression of height h.
func (p *parser) finished(h int) { p.height = max(p.height, h) }

// literalChild records a literal child that was not parsed as an
// expression, as in DATE '2020-01-01'.
func (p *parser) literalChild() {
	p.elements++
	p.finished(1)
}

// collection makes collection e, of n elements, one node of height 1 when e
// is a collection of literals. VALUES rows are not counted, so there only
// the height changes.
func (p *parser) collection(e Expr, n int) {
	if !isArrayLiteral(e) && !isTupleLiteral(e) {
		return
	}
	p.height = 0
	if !p.inValues {
		p.elements -= n
	}
}

// node counts a node over children of the given heights and returns its
// height.
func (p *parser) node(children ...int) int {
	h := 0
	for _, c := range children {
		h = max(h, c)
	}
	p.elements++
	return p.checkHeight(h + 1)
}

func (p *parser) checkHeight(h int) int {
	if h > limits.MaxASTDepth {
		panic(bail{cherr.New(cherr.TooDeepAST, "AST is too deep. Maximum: %d", limits.MaxASTDepth)})
	}
	return h
}

// mergesChain reports whether op continues a chain of the same operator,
// which ClickHouse merges into one function: a AND b AND c is and(a, b, c).
func mergesChain(op, prev string) bool {
	return op == prev && (op == "AND" || op == "OR" || op == "||")
}

// checkElements enforces max_ast_elements once the statement is parsed, as
// ClickHouse does.
func (p *parser) checkElements() {
	if p.elements > limits.MaxASTElements {
		panic(bail{cherr.New(cherr.TooBigAST, "AST is too big. Maximum: %d", limits.MaxASTElements)})
	}
}
