package cherr

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Names reported by ClickHouse 25.8 (errorCodeToName) for codes the code
// base raises.
func TestNames(t *testing.T) {
	cases := map[int]string{
		ThereIsNoColumn:                  "THERE_IS_NO_COLUMN",
		DuplicateColumn:                  "DUPLICATE_COLUMN",
		CannotParseEscapeSequence:        "CANNOT_PARSE_ESCAPE_SEQUENCE",
		CannotParseQuotedString:          "CANNOT_PARSE_QUOTED_STRING",
		CannotParseInputAssertionFailed:  "CANNOT_PARSE_INPUT_ASSERTION_FAILED",
		CannotParseDate:                  "CANNOT_PARSE_DATE",
		ArgumentOutOfBound:               "ARGUMENT_OUT_OF_BOUND",
		CannotParseNumber:                "CANNOT_PARSE_NUMBER",
		IncorrectResultOfScalarSubquery:  "INCORRECT_RESULT_OF_SCALAR_SUBQUERY",
		CannotReadArrayFromText:          "CANNOT_READ_ARRAY_FROM_TEXT",
		TooLargeStringSize:               "TOO_LARGE_STRING_SIZE",
		IllegalDivision:                  "ILLEGAL_DIVISION",
		IllegalAggregation:               "ILLEGAL_AGGREGATION",
		SizesOfArraysDontMatch:           "SIZES_OF_ARRAYS_DONT_MATCH",
		AmbiguousIdentifier:              "AMBIGUOUS_IDENTIFIER",
		UnexpectedASTStructure:           "UNEXPECTED_AST_STRUCTURE",
		UnionAllResultStructuresMismatch: "UNION_ALL_RESULT_STRUCTURES_MISMATCH",
		TooDeepRecursion:                 "TOO_DEEP_RECURSION",
		CannotInsertNullInOrdinaryColumn: "CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN",
		CannotParseUUID:                  "CANNOT_PARSE_UUID",
		DataTypeCannotHaveArguments:      "DATA_TYPE_CANNOT_HAVE_ARGUMENTS",
		NoCommonType:                     "NO_COMMON_TYPE",
		TooManyRowsOrBytes:               "TOO_MANY_ROWS_OR_BYTES",
		DecimalOverflow:                  "DECIMAL_OVERFLOW",
		CannotParseBool:                  "CANNOT_PARSE_BOOL",
		ExpectedAllOrDistinct:            "EXPECTED_ALL_OR_DISTINCT",
		UnknownElementOfEnum:             "UNKNOWN_ELEMENT_OF_ENUM",
		Timeout:                          "TIMEOUT_EXCEEDED",
		ReadonlyQuery:                    "READONLY",
		QueryWasCancelled:                "QUERY_WAS_CANCELLED",
		Unavailable:                      "KEEPER_EXCEPTION",
		StdException:                     "STD_EXCEPTION",
		UnknownTable:                     "UNKNOWN_TABLE",
	}
	for code, want := range cases {
		if got := Name(code); got != want {
			t.Errorf("Name(%d) = %q, want %q", code, got, want)
		}
		if got := New(code, "x").Name(); got != want {
			t.Errorf("Error{%d}.Name() = %q, want %q", code, got, want)
		}
	}
	if got := Name(123456); got != "UNKNOWN_EXCEPTION" {
		t.Errorf("Name(unknown) = %q", got)
	}
	if got := New(IllegalDivision, "Division by zero").Error(); got != "Code: 153. DB::Exception: Division by zero. (ILLEGAL_DIVISION)" {
		t.Errorf("Error() = %q", got)
	}
}

// Statuses verified against ClickHouse 25.8 with
// SELECT throwIf(1, 'x', toInt32(code)) SETTINGS allow_custom_error_code_in_throwif=1.
func TestHTTPStatus(t *testing.T) {
	want := map[int][]int{
		400: {6, 8, 15, 25, 26, 27, 36, 37, 38, 41, 44, 53, 62, 72, 117, 123, 167, 168, 223, 321, 376, 427, 441, 675, 676},
		401: {194},
		403: {192, 193, 512, 516},
		404: {46, 47, 50, 56, 60, 63, 73, 78, 81, 115, 152, 336, 511},
		408: {159},
		411: {381},
		413: {229},
		501: {48},
		503: {76, 209, 439},
	}
	special := map[int]int{}
	for status, codes := range want {
		for _, c := range codes {
			special[c] = status
		}
	}
	for code := range names {
		exp, ok := special[code]
		if !ok {
			exp = http.StatusInternalServerError
		}
		if got := HTTPStatus(code); got != exp {
			t.Errorf("HTTPStatus(%d %s) = %d, want %d", code, names[code], got, exp)
		}
		if got := (&Error{Code: code}).HTTPStatus(); got != exp {
			t.Errorf("Error{%d}.HTTPStatus() = %d, want %d", code, got, exp)
		}
	}
	// Codes ClickHouse does not define fall back to 500 too.
	if got := HTTPStatus(StdException + 5000); got != 500 {
		t.Errorf("HTTPStatus(undefined) = %d", got)
	}
}

// TestRaisedCodesHaveNames scans the code base for error codes passed to
// cherr.New / cherr.Wrap / cherr.Error{Code: ...} — as integer literals or as
// package-level integer constants — and checks that each has a ClickHouse
// name, so no client ever sees UNKNOWN_EXCEPTION for a code we raise.
func TestRaisedCodesHaveNames(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	cherrConsts := 0
	for _, dir := range []string{"internal", "cmd"} {
		pkgFiles := map[string][]*ast.File{}
		fset := token.NewFileSet()
		if _, err := os.Stat(filepath.Join(root, dir)); errors.Is(err, fs.ErrNotExist) {
			continue
		}
		err := filepath.WalkDir(filepath.Join(root, dir), func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return err
			}
			f, err := parser.ParseFile(fset, p, nil, 0)
			if err != nil {
				return err
			}
			pkgFiles[filepath.Dir(p)] = append(pkgFiles[filepath.Dir(p)], f)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		for pkgDir, files := range pkgFiles {
			consts := intConsts(files)
			if filepath.Base(pkgDir) == "cherr" {
				cherrConsts += len(consts)
				for name, v := range consts {
					if _, ok := names[v]; !ok {
						t.Errorf("cherr.%s = %d has no ClickHouse name", name, v)
					}
				}
				continue
			}
			for _, f := range files {
				ast.Inspect(f, func(n ast.Node) bool {
					var arg ast.Expr
					switch x := n.(type) {
					case *ast.CallExpr:
						if sel, ok := x.Fun.(*ast.SelectorExpr); ok && isIdent(sel.X, "cherr") &&
							(sel.Sel.Name == "New" || sel.Sel.Name == "Wrap" || sel.Sel.Name == "Is") && len(x.Args) > 0 {
							arg = x.Args[0]
							if sel.Sel.Name == "Is" && len(x.Args) > 1 {
								arg = x.Args[1]
							}
						}
					case *ast.CompositeLit:
						if sel, ok := x.Type.(*ast.SelectorExpr); ok && isIdent(sel.X, "cherr") && sel.Sel.Name == "Error" {
							for _, el := range x.Elts {
								if kv, ok := el.(*ast.KeyValueExpr); ok && isIdent(kv.Key, "Code") {
									arg = kv.Value
								}
							}
						}
					}
					if arg == nil {
						return true
					}
					code, ok := -1, false
					switch a := arg.(type) {
					case *ast.BasicLit:
						if a.Kind == token.INT {
							v, err := strconv.Atoi(a.Value)
							code, ok = v, err == nil
						}
					case *ast.Ident:
						code, ok = consts[a.Name]
					}
					if ok {
						if _, named := names[code]; !named {
							t.Errorf("%s: code %d has no ClickHouse name", fset.Position(arg.Pos()), code)
						}
					}
					return true
				})
			}
		}
	}
	// The scan must at least see cherr's own code constants; otherwise the
	// walker or the constant resolver is broken and the check proves nothing.
	if cherrConsts == 0 {
		t.Fatal("scan found no cherr code constants; the scanner is broken")
	}
}

func isIdent(e ast.Expr, name string) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Name == name
}

// intConsts returns the package-level constants initialised with an integer
// literal.
func intConsts(files []*ast.File) map[string]int {
	out := map[string]int{}
	for _, f := range files {
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, sp := range gd.Specs {
				vs := sp.(*ast.ValueSpec)
				for i, n := range vs.Names {
					if i >= len(vs.Values) {
						continue
					}
					if lit, ok := vs.Values[i].(*ast.BasicLit); ok && lit.Kind == token.INT {
						if v, err := strconv.Atoi(lit.Value); err == nil {
							out[n.Name] = v
						}
					}
				}
			}
		}
	}
	return out
}
