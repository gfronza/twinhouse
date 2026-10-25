def golit(b):
    if isinstance(b, str): b = b.encode()
    out = ['"']
    for c in b:
        ch = chr(c)
        if ch == '"': out.append('\\"')
        elif ch == '\\': out.append('\\\\')
        elif 0x20 <= c < 0x7f: out.append(ch)
        elif ch == '\n': out.append('\\n')
        elif ch == '\t': out.append('\\t')
        else: out.append('\\x%02x' % c)
    out.append('"')
    return ''.join(out)
