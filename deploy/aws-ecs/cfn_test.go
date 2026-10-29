package deployawsecs_test

import (
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"
)

// This file parses template.yaml with the standard library only: a reader
// for the YAML subset the template uses (block mappings and sequences,
// flow sequences and mappings, quoted and plain scalars, folded scalars,
// comments, CloudFormation short-form tags) and an evaluator of the
// intrinsic functions the container definitions use (Ref, Fn::If,
// Fn::Join, Fn::Sub, conditions). It is deliberately strict: anything
// outside the subset is an error, so a template change that needs more is
// noticed instead of mis-parsed.

// node is a parsed YAML value: string, []any or map[string]any.
type node = any

type yline struct {
	no     int // 1-based line number in the file
	indent int
	text   string // without indentation and comment
}

// parseYAML parses the subset of YAML described above.
func parseYAML(src string) (map[string]any, error) {
	var lines []yline
	raw := strings.Split(strings.ReplaceAll(src, "\r\n", "\n"), "\n")
	for i := 0; i < len(raw); i++ {
		l := raw[i]
		if strings.Contains(l, "\t") {
			return nil, fmt.Errorf("line %d: tab character", i+1)
		}
		text := stripComment(l)
		trimmed := strings.TrimLeft(text, " ")
		if strings.TrimSpace(trimmed) == "" {
			continue
		}
		indent := len(text) - len(trimmed)
		trimmed = strings.TrimRight(trimmed, " ")
		// Block scalars (">-", ">", "|", "|-") are folded into one line here.
		if k, style, ok := blockScalarHeader(trimmed); ok {
			var parts []string
			j := i + 1
			for ; j < len(raw); j++ {
				t := strings.TrimLeft(raw[j], " ")
				if t == "" {
					parts = append(parts, "")
					continue
				}
				if len(raw[j])-len(t) <= indent {
					break
				}
				parts = append(parts, strings.TrimRight(t, " "))
			}
			for len(parts) > 0 && parts[len(parts)-1] == "" {
				parts = parts[:len(parts)-1]
			}
			sep := " "
			if strings.HasPrefix(style, "|") {
				sep = "\n"
			}
			lines = append(lines, yline{no: i + 1, indent: indent, text: k + quoteDouble(strings.Join(parts, sep))})
			i = j - 1
			continue
		}
		lines = append(lines, yline{no: i + 1, indent: indent, text: trimmed})
	}
	p := &yparser{lines: lines}
	if len(lines) == 0 {
		return nil, fmt.Errorf("empty document")
	}
	v, err := p.block(lines[0].indent)
	if err != nil {
		return nil, err
	}
	if p.i != len(lines) {
		return nil, fmt.Errorf("line %d: unexpected content", lines[p.i].no)
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("top level is not a mapping")
	}
	return m, nil
}

// blockScalarHeader recognizes "key: >-" and "- >-" (the prefix up to the
// indicator is returned so the folded text can be appended to it).
func blockScalarHeader(t string) (prefix, style string, ok bool) {
	for _, s := range []string{">-", ">", "|-", "|"} {
		if strings.HasSuffix(t, ": "+s) {
			return strings.TrimSuffix(t, s), s, true
		}
		if t == "- "+s {
			return "- ", s, true
		}
	}
	return "", "", false
}

func quoteDouble(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s) + `"`
}

// stripComment removes a "#" comment that is not inside quotes.
func stripComment(l string) string {
	var q byte
	for i := 0; i < len(l); i++ {
		c := l[i]
		switch {
		case q == '\'' && c == '\'':
			if i+1 < len(l) && l[i+1] == '\'' {
				i++
				continue
			}
			q = 0
		case q == '"' && c == '\\':
			i++
		case q == '"' && c == '"':
			q = 0
		case q != 0:
		case c == '\'' || c == '"':
			if i == 0 || strings.ContainsRune(" [,{:", rune(l[i-1])) {
				q = c
			}
		case c == '#' && (i == 0 || l[i-1] == ' '):
			return l[:i]
		}
	}
	return l
}

type yparser struct {
	lines []yline
	i     int
}

func (p *yparser) errf(format string, a ...any) error {
	no := 0
	if p.i < len(p.lines) {
		no = p.lines[p.i].no
	}
	return fmt.Errorf("line %d: %s", no, fmt.Sprintf(format, a...))
}

// block parses the mapping or sequence whose lines are at indent.
func (p *yparser) block(indent int) (node, error) {
	if p.i >= len(p.lines) {
		return nil, p.errf("missing block")
	}
	l := p.lines[p.i]
	if l.indent != indent {
		return nil, p.errf("indentation %d, want %d", l.indent, indent)
	}
	if l.text == "-" || strings.HasPrefix(l.text, "- ") {
		return p.sequence(indent)
	}
	return p.mapping(indent)
}

func (p *yparser) sequence(indent int) (node, error) {
	var out []any
	for p.i < len(p.lines) && p.lines[p.i].indent == indent && (p.lines[p.i].text == "-" || strings.HasPrefix(p.lines[p.i].text, "- ")) {
		rest := strings.TrimSpace(strings.TrimPrefix(p.lines[p.i].text, "-"))
		switch {
		case rest == "":
			p.i++
			v, err := p.child(indent)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		case isLoneTag(rest):
			p.i++
			v, err := p.child(indent)
			if err != nil {
				return nil, err
			}
			out = append(out, applyTag(rest, v))
		default:
			if k, v, ok := splitKey(rest); ok {
				// "- key: value" starts a mapping whose other keys are
				// indented to the first key's column.
				p.lines[p.i] = yline{no: p.lines[p.i].no, indent: indent + 2, text: k + ":" + prefixSpace(v)}
				m, err := p.mapping(indent + 2)
				if err != nil {
					return nil, err
				}
				out = append(out, m)
				continue
			}
			v, err := parseFlow(rest)
			if err != nil {
				return nil, p.errf("%v", err)
			}
			out = append(out, v)
			p.i++
		}
	}
	return out, nil
}

func prefixSpace(s string) string {
	if s == "" {
		return ""
	}
	return " " + s
}

// child parses the block below the current line: more indented, or a
// sequence at the same indentation as a mapping key ("key:\n- a").
func (p *yparser) child(parentIndent int) (node, error) {
	if p.i >= len(p.lines) || p.lines[p.i].indent <= parentIndent {
		return nil, p.errf("missing nested block")
	}
	return p.block(p.lines[p.i].indent)
}

func (p *yparser) mapping(indent int) (node, error) {
	out := map[string]any{}
	for p.i < len(p.lines) && p.lines[p.i].indent == indent {
		l := p.lines[p.i]
		if strings.HasPrefix(l.text, "- ") || l.text == "-" {
			return nil, p.errf("sequence item inside a mapping")
		}
		k, rest, ok := splitKey(l.text)
		if !ok {
			return nil, p.errf("expected key: value, got %q", l.text)
		}
		if _, dup := out[k]; dup {
			return nil, p.errf("duplicate key %q", k)
		}
		p.i++
		switch {
		case rest == "":
			if p.i < len(p.lines) && p.lines[p.i].indent == indent && strings.HasPrefix(p.lines[p.i].text, "- ") {
				v, err := p.sequence(indent)
				if err != nil {
					return nil, err
				}
				out[k] = v
				continue
			}
			v, err := p.child(indent)
			if err != nil {
				return nil, err
			}
			out[k] = v
		case isLoneTag(rest):
			v, err := p.child(indent)
			if err != nil {
				return nil, err
			}
			out[k] = applyTag(rest, v)
		default:
			p.i--
			v, err := parseFlow(rest)
			if err != nil {
				return nil, p.errf("%v", err)
			}
			p.i++
			out[k] = v
		}
	}
	return out, nil
}

// splitKey splits "key: value" / "key:" at the first ": " outside quotes
// and brackets. Keys are plain (they may contain ':' and '-', e.g.
// aws:SourceArn, awslogs-group).
func splitKey(t string) (key, rest string, ok bool) {
	if t == "" || strings.ContainsRune("'\"[{!", rune(t[0])) {
		return "", "", false
	}
	if strings.HasSuffix(t, ":") && !strings.Contains(t, ": ") {
		return strings.TrimSuffix(t, ":"), "", true
	}
	i := strings.Index(t, ": ")
	if i <= 0 {
		return "", "", false
	}
	key = t[:i]
	if strings.ContainsAny(key, " []{},") {
		return "", "", false
	}
	return key, strings.TrimSpace(t[i+2:]), true
}

func isLoneTag(s string) bool {
	return strings.HasPrefix(s, "!") && !strings.ContainsAny(s, " [{'\"")
}

// applyTag turns a CloudFormation short-form tag into its long form.
func applyTag(tag string, v node) node {
	name := strings.TrimPrefix(tag, "!")
	switch name {
	case "Ref", "Condition":
		return map[string]any{name: v}
	case "GetAtt":
		if s, ok := v.(string); ok {
			res, attr, _ := strings.Cut(s, ".")
			return map[string]any{"Fn::GetAtt": []any{res, attr}}
		}
		return map[string]any{"Fn::GetAtt": v}
	}
	return map[string]any{"Fn::" + name: v}
}

// parseFlow parses an inline value: a scalar, a flow sequence or mapping,
// optionally tagged.
func parseFlow(s string) (node, error) {
	fp := &flowParser{s: s}
	v, err := fp.value()
	if err != nil {
		return nil, err
	}
	fp.space()
	if fp.i != len(fp.s) {
		return nil, fmt.Errorf("trailing %q in %q", fp.s[fp.i:], s)
	}
	return v, nil
}

type flowParser struct {
	s     string
	i     int
	depth int
}

func (f *flowParser) space() {
	for f.i < len(f.s) && f.s[f.i] == ' ' {
		f.i++
	}
}

func (f *flowParser) value() (node, error) {
	f.space()
	if f.i >= len(f.s) {
		return "", nil
	}
	switch c := f.s[f.i]; c {
	case '!':
		j := f.i
		for j < len(f.s) && f.s[j] != ' ' && f.s[j] != ',' && f.s[j] != ']' {
			j++
		}
		tag := f.s[f.i:j]
		f.i = j
		v, err := f.value()
		if err != nil {
			return nil, err
		}
		return applyTag(tag, v), nil
	case '[':
		f.i++
		f.depth++
		var out []any
		for {
			f.space()
			if f.i < len(f.s) && f.s[f.i] == ']' {
				f.i++
				f.depth--
				return out, nil
			}
			v, err := f.value()
			if err != nil {
				return nil, err
			}
			out = append(out, v)
			f.space()
			if f.i < len(f.s) && f.s[f.i] == ',' {
				f.i++
				continue
			}
			if f.i < len(f.s) && f.s[f.i] == ']' {
				continue
			}
			return nil, fmt.Errorf("unterminated flow sequence in %q", f.s)
		}
	case '{':
		f.i++
		f.depth++
		out := map[string]any{}
		for {
			f.space()
			if f.i < len(f.s) && f.s[f.i] == '}' {
				f.i++
				f.depth--
				return out, nil
			}
			j := strings.Index(f.s[f.i:], ": ")
			if j < 0 {
				return nil, fmt.Errorf("bad flow mapping in %q", f.s)
			}
			k := strings.TrimSpace(f.s[f.i : f.i+j])
			f.i += j + 2
			v, err := f.value()
			if err != nil {
				return nil, err
			}
			out[k] = v
			f.space()
			if f.i < len(f.s) && f.s[f.i] == ',' {
				f.i++
			}
		}
	case '\'':
		var b strings.Builder
		for j := f.i + 1; j < len(f.s); j++ {
			if f.s[j] == '\'' {
				if j+1 < len(f.s) && f.s[j+1] == '\'' {
					b.WriteByte('\'')
					j++
					continue
				}
				f.i = j + 1
				return b.String(), nil
			}
			b.WriteByte(f.s[j])
		}
		return nil, fmt.Errorf("unterminated single-quoted string in %q", f.s)
	case '"':
		var b strings.Builder
		for j := f.i + 1; j < len(f.s); j++ {
			switch f.s[j] {
			case '\\':
				j++
				if j < len(f.s) {
					switch f.s[j] {
					case 'n':
						b.WriteByte('\n')
					default:
						b.WriteByte(f.s[j])
					}
				}
			case '"':
				f.i = j + 1
				return b.String(), nil
			default:
				b.WriteByte(f.s[j])
			}
		}
		return nil, fmt.Errorf("unterminated double-quoted string in %q", f.s)
	default:
		j := f.i
		for j < len(f.s) {
			if f.depth > 0 && (f.s[j] == ',' || f.s[j] == ']' || f.s[j] == '}') {
				break
			}
			j++
		}
		v := strings.TrimSpace(f.s[f.i:j])
		f.i = j
		return v, nil
	}
}

// ---------------------------------------------------------------------------
// Template access and intrinsic-function evaluation.
// ---------------------------------------------------------------------------

type template struct {
	root       map[string]any
	params     map[string]any
	conditions map[string]any
	resources  map[string]any
	outputs    map[string]any
}

func loadTemplate(t testing.TB) *template {
	t.Helper()
	b, err := os.ReadFile("template.yaml")
	if err != nil {
		t.Fatal(err)
	}
	root, err := parseYAML(string(b))
	if err != nil {
		t.Fatalf("template.yaml: %v", err)
	}
	tp := &template{root: root}
	tp.params, _ = root["Parameters"].(map[string]any)
	tp.conditions, _ = root["Conditions"].(map[string]any)
	tp.resources, _ = root["Resources"].(map[string]any)
	tp.outputs, _ = root["Outputs"].(map[string]any)
	if tp.params == nil || tp.conditions == nil || tp.resources == nil || tp.outputs == nil {
		t.Fatal("template lacks Parameters, Conditions, Resources or Outputs")
	}
	return tp
}

func (tp *template) resource(t testing.TB, name string) map[string]any {
	t.Helper()
	r, ok := tp.resources[name].(map[string]any)
	if !ok {
		t.Fatalf("resource %s missing", name)
	}
	return r
}

func (tp *template) resourcesOfType(typ string) []string {
	var out []string
	for name, r := range tp.resources {
		if m, ok := r.(map[string]any); ok && m["Type"] == typ {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// get walks a path of map keys and list indexes ("Properties", "Port").
func get(v node, path ...any) node {
	for _, p := range path {
		switch k := p.(type) {
		case string:
			m, ok := v.(map[string]any)
			if !ok {
				return nil
			}
			v = m[k]
		case int:
			l, ok := v.([]any)
			if !ok || k >= len(l) {
				return nil
			}
			v = l[k]
		}
	}
	return v
}

// stack is a template instantiated with parameter values in a region.
type stack struct {
	tp        *template
	region    string
	account   string
	name      string
	values    map[string]any // string or []string
	condCache map[string]bool
}

// noValue is AWS::NoValue: removed from the enclosing list or mapping.
type noValueT struct{}

var noValue = noValueT{}

func (tp *template) instantiate(t testing.TB, region string, overrides map[string]string) *stack {
	t.Helper()
	s := &stack{tp: tp, region: region, account: "123456789012", name: "twinhouse", values: map[string]any{}, condCache: map[string]bool{}}
	for name, p := range tp.params {
		pm := p.(map[string]any)
		v, ok := overrides[name]
		if !ok {
			def, has := pm["Default"].(string)
			if !has {
				t.Fatalf("parameter %s has no default and no value", name)
			}
			v = def
		}
		typ, _ := pm["Type"].(string)
		if typ == "CommaDelimitedList" || strings.HasPrefix(typ, "List<") {
			var l []string
			for _, e := range strings.Split(v, ",") {
				l = append(l, strings.TrimSpace(e))
			}
			s.values[name] = l
		} else {
			s.values[name] = v
		}
	}
	for name := range overrides {
		if _, ok := tp.params[name]; !ok {
			t.Fatalf("override of unknown parameter %s", name)
		}
	}
	return s
}

func (s *stack) cond(t testing.TB, name string) bool {
	t.Helper()
	if v, ok := s.condCache[name]; ok {
		return v
	}
	c, ok := s.tp.conditions[name]
	if !ok {
		t.Fatalf("unknown condition %s", name)
	}
	v := s.evalCond(t, c)
	s.condCache[name] = v
	return v
}

func (s *stack) evalCond(t testing.TB, c node) bool {
	t.Helper()
	m, ok := c.(map[string]any)
	if !ok || len(m) != 1 {
		t.Fatalf("bad condition %v", c)
	}
	for fn, arg := range m {
		args, _ := arg.([]any)
		switch fn {
		case "Condition":
			return s.cond(t, arg.(string))
		case "Fn::Equals":
			a, b := s.eval(t, args[0]), s.eval(t, args[1])
			return fmt.Sprint(a) == fmt.Sprint(b)
		case "Fn::Not":
			return !s.evalCond(t, args[0])
		case "Fn::And":
			for _, a := range args {
				if !s.evalCond(t, a) {
					return false
				}
			}
			return true
		case "Fn::Or":
			for _, a := range args {
				if s.evalCond(t, a) {
					return true
				}
			}
			return false
		}
		t.Fatalf("unsupported condition function %s", fn)
	}
	return false
}

// eval resolves intrinsic functions. Refs to resources and GetAtt become
// "<Name>" / "<Name.Attr>" placeholders.
func (s *stack) eval(t testing.TB, v node) node {
	t.Helper()
	switch x := v.(type) {
	case string:
		return x
	case []any:
		var out []any
		for _, e := range x {
			r := s.eval(t, e)
			if r == noValue {
				continue
			}
			out = append(out, r)
		}
		return out
	case map[string]any:
		if len(x) == 1 {
			for fn, arg := range x {
				switch fn {
				case "Ref":
					return s.ref(t, arg.(string))
				case "Fn::GetAtt":
					a := arg.([]any)
					return fmt.Sprintf("<%s.%s>", a[0], a[1])
				case "Fn::If":
					a := arg.([]any)
					if s.cond(t, a[0].(string)) {
						return s.eval(t, a[1])
					}
					return s.eval(t, a[2])
				case "Fn::Join":
					a := arg.([]any)
					return strings.Join(toStrings(t, s.eval(t, a[1])), a[0].(string))
				case "Fn::Sub":
					return s.sub(t, arg)
				}
				if strings.HasPrefix(fn, "Fn::") {
					t.Fatalf("unsupported intrinsic %s", fn)
				}
			}
		}
		out := map[string]any{}
		for k, e := range x {
			r := s.eval(t, e)
			if r == noValue {
				continue
			}
			out[k] = r
		}
		return out
	}
	return v
}

func toStrings(t testing.TB, v node) []string {
	t.Helper()
	switch x := v.(type) {
	case []string:
		return x
	case []any:
		var out []string
		for _, e := range x {
			out = append(out, toStrings(t, e)...)
		}
		return out
	case string:
		return []string{x}
	}
	t.Fatalf("not a list of strings: %#v", v)
	return nil
}

func (s *stack) ref(t testing.TB, name string) node {
	t.Helper()
	switch name {
	case "AWS::NoValue":
		return noValue
	case "AWS::Region":
		return s.region
	case "AWS::AccountId":
		return s.account
	case "AWS::StackName":
		return s.name
	case "AWS::Partition":
		return "aws"
	}
	if v, ok := s.values[name]; ok {
		if l, ok := v.([]string); ok {
			out := make([]any, len(l))
			for i, e := range l {
				out[i] = e
			}
			return out
		}
		return v
	}
	if _, ok := s.tp.resources[name]; ok {
		return "<" + name + ">"
	}
	t.Fatalf("Ref to unknown name %s", name)
	return nil
}

func (s *stack) sub(t testing.TB, arg node) string {
	t.Helper()
	var str string
	vars := map[string]any{}
	switch a := arg.(type) {
	case string:
		str = a
	case []any:
		str = a[0].(string)
		for k, v := range a[1].(map[string]any) {
			vars[k] = s.eval(t, v)
		}
	}
	var b strings.Builder
	for {
		i := strings.Index(str, "${")
		if i < 0 {
			b.WriteString(str)
			return b.String()
		}
		b.WriteString(str[:i])
		j := strings.Index(str[i:], "}")
		name := str[i+2 : i+j]
		str = str[i+j+1:]
		if v, ok := vars[name]; ok {
			fmt.Fprint(&b, v)
		} else if res, attr, ok := strings.Cut(name, "."); ok && !strings.HasPrefix(name, "AWS::") {
			fmt.Fprintf(&b, "<%s.%s>", res, attr)
		} else {
			fmt.Fprint(&b, s.ref(t, name))
		}
	}
}

// failedRules evaluates the template's Rules for this stack's parameters
// and returns the AssertDescription of every assertion that fails.
func (s *stack) failedRules(t testing.TB) []string {
	t.Helper()
	rules, _ := s.tp.root["Rules"].(map[string]any)
	var failed []string
	for name, r := range rules {
		rm := r.(map[string]any)
		if c, ok := rm["RuleCondition"]; ok && !s.evalRule(t, c) {
			continue
		}
		as, _ := rm["Assertions"].([]any)
		if len(as) == 0 {
			t.Fatalf("rule %s has no assertions", name)
		}
		for _, a := range as {
			am := a.(map[string]any)
			if !s.evalRule(t, am["Assert"]) {
				failed = append(failed, fmt.Sprint(am["AssertDescription"]))
			}
		}
	}
	sort.Strings(failed)
	return failed
}

// evalRule evaluates a rule condition or assertion. Only the functions
// CloudFormation allows in Rules are accepted.
func (s *stack) evalRule(t testing.TB, c node) bool {
	t.Helper()
	m, ok := c.(map[string]any)
	if !ok || len(m) != 1 {
		t.Fatalf("bad rule expression %v", c)
	}
	for fn, arg := range m {
		args, _ := arg.([]any)
		switch fn {
		case "Fn::Equals":
			return fmt.Sprint(s.eval(t, args[0])) == fmt.Sprint(s.eval(t, args[1]))
		case "Fn::Not":
			return !s.evalRule(t, args[0])
		case "Fn::And":
			for _, a := range args {
				if !s.evalRule(t, a) {
					return false
				}
			}
			return true
		case "Fn::Or":
			for _, a := range args {
				if s.evalRule(t, a) {
					return true
				}
			}
			return false
		case "Fn::Contains":
			return slices.Contains(toStrings(t, s.eval(t, args[0])), fmt.Sprint(s.eval(t, args[1])))
		case "Fn::EachMemberEquals":
			want := fmt.Sprint(s.eval(t, args[1]))
			for _, e := range toStrings(t, s.eval(t, args[0])) {
				if e != want {
					return false
				}
			}
			return true
		}
		t.Fatalf("function %s is not allowed in Rules (or not supported here)", fn)
	}
	return false
}

// exists reports whether a resource is created in this stack.
func (s *stack) exists(t testing.TB, name string) bool {
	t.Helper()
	r := s.tp.resource(t, name)
	c, ok := r["Condition"].(string)
	return !ok || s.cond(t, c)
}

// container is one rendered container definition.
type container struct {
	Name        string
	Image       string
	Command     []string
	Env         map[string]string
	EnvOrder    []string
	Secrets     map[string]string
	StopTimeout string
	Ports       []string
	ReadOnly    bool
	Health      []string
}

// containers renders the container definitions of a task definition.
func (s *stack) containers(t testing.TB, taskDef string) []container {
	t.Helper()
	defs := s.eval(t, get(s.tp.resource(t, taskDef), "Properties", "ContainerDefinitions")).([]any)
	var out []container
	for _, d := range defs {
		m := d.(map[string]any)
		c := container{Env: map[string]string{}, Secrets: map[string]string{}}
		c.Name, _ = m["Name"].(string)
		c.Image, _ = m["Image"].(string)
		c.Command = toStrings(t, m["Command"])
		c.StopTimeout, _ = m["StopTimeout"].(string)
		c.ReadOnly = m["ReadonlyRootFilesystem"] == "true"
		if env, ok := m["Environment"].([]any); ok {
			for _, e := range env {
				em := e.(map[string]any)
				name, value := em["Name"].(string), fmt.Sprint(em["Value"])
				if _, dup := c.Env[name]; dup {
					t.Errorf("%s/%s: %s set twice", taskDef, c.Name, name)
				}
				c.Env[name] = value
				c.EnvOrder = append(c.EnvOrder, name)
			}
		}
		if sec, ok := m["Secrets"].([]any); ok {
			for _, e := range sec {
				em := e.(map[string]any)
				c.Secrets[em["Name"].(string)] = fmt.Sprint(em["ValueFrom"])
			}
		}
		if pm, ok := m["PortMappings"].([]any); ok {
			for _, p := range pm {
				c.Ports = append(c.Ports, fmt.Sprint(get(p, "ContainerPort")))
			}
		}
		if hc := get(m, "HealthCheck", "Command"); hc != nil {
			c.Health = toStrings(t, hc)
		}
		out = append(out, c)
	}
	return out
}
