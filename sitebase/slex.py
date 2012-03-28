#!/usr/bin/env python
# -*- mode: python -*-

# A simple lex parser

from collections import namedtuple

__all__ = ["parse",
           "lex",
           "ParseError",
           "TOKEN_PARENTHESIS",
           "TOKEN_LOGIC",
           "TOKEN_OPER",
           "TOKEN_VALUE"]

TOKEN_PARENTHESIS = 3
TOKEN_LOGIC        = 2
TOKEN_OPER         = 1
TOKEN_VALUE        = 0

Operator = namedtuple("Operator", ["p", "assoc"])

OPERS = {
    "OR": Operator(p=1, assoc="L"),
    "AND": Operator(p=2, assoc="L"),
    "===": Operator(p=3, assoc="L"),
    "!==": Operator(p=3, assoc="L"),
    "==": Operator(p=3, assoc="L"),
    "!=": Operator(p=3, assoc="L"),
    "IN": Operator(p=3, assoc="L"),
    "^": Operator(p=3, assoc="L"),
    "~": Operator(p=3, assoc="L"),
    "!~": Operator(p=3, assoc="L"),
    "<": Operator(p=4, assoc="L"),
    "<=": Operator(p=4, assoc="L"),
    ">": Operator(p=4, assoc="L"),
    ">=": Operator(p=4, assoc="L"),
}

LOGIC_OPERS = ("OR", "AND")
QUOTES = ("'", '"')
WHITESPACES = (" ", "\t", "\r", "\n")
OPCHARS = ("!", "=", "^", "~", ">", "<")


class ParseError(Exception):
    pass


def lex(q):
    tokens, i, last, imax = [], 0, 0, len(q)

    def __add_token(q, tokens, last, i, literal=False):
        t = q[last:i]

        if literal:
            tokens.append((t, TOKEN_VALUE, last))
        elif t and t.upper() in LOGIC_OPERS:
            tokens.append((t.upper(), TOKEN_LOGIC, last))
        elif t and t[0] in OPCHARS:
            tokens.append((t, TOKEN_OPER, last))
        elif t and t[0] in ("(", ")"):
            tokens.append((t, TOKEN_PARENTHESIS, last))
        elif t and t.upper() in ("IN"):
            tokens.append((t, TOKEN_OPER, last))
        elif t:
            tokens.append((t, TOKEN_VALUE, last))

        return i

    while i < imax:
        ch = q[i]
        if ch in QUOTES:

            last = i = __add_token(q, tokens, last, i) + 1

            # consume chars in quotes
            while i < imax and q[i] not in QUOTES:
                i = i + 1

            last = i = __add_token(q, tokens, last, i, True) + 1

        elif ch in WHITESPACES:

            __add_token(q, tokens, last, i)

            # skip whitespaces
            while i < imax and q[i] in WHITESPACES:
                i = i + 1

            last = i

        elif ch in OPCHARS:

            last = i = __add_token(q, tokens, last, i)

            # consume potential operators
            while i < imax and q[i] in OPCHARS:
                i = i + 1

            last = __add_token(q, tokens, last, i)
        elif ch in ("(", ")"):
            last = __add_token(q, tokens, last, i)
            last = i = __add_token(q, tokens, last, last + 1)
        else:
            i = i + 1

    __add_token(q, tokens, last, i)
    return tokens


def parse(q):
    """http://en.wikipedia.org/wiki/Shunting-yard_algorithm"""

    def _merge(output, scache, pos):
        if scache:
            s = " ".join(scache)
            output.append((s, TOKEN_VALUE, pos - len(s)))
            del scache[:]

    try:
        tokens = lex(q)
    except Exception as e:
        raise ParseError(e.message)

    tokens.reverse()
    scache, stack, output = list(), list(), list()
    while tokens:
        tup = tokens.pop()
        token, token_type, pos = tup[0], tup[1], tup[2]
        utoken = token.upper()
        if token_type in (TOKEN_OPER, TOKEN_LOGIC):
            _merge(output, scache, pos)
            if stack and not (stack[-1][1] == TOKEN_PARENTHESIS
                              and stack[-1][0] == "("):
                # compare with old token on the top of stack
                top = stack[-1]
                if utoken not in OPERS:
                    raise ParseError(
                        "invalid operator `%s' at position %s" % (token, pos))
                p = (OPERS[utoken], OPERS[top[0]])
                if ((p[0].assoc == "L" and p[0].p <= p[1].p) or
                    (p[0].assoc == "R" and p[0].p < p[1].p)):
                    output.append(stack.pop())

            # push new token onto stack
            if token_type == TOKEN_LOGIC:
                stack.append((utoken, TOKEN_LOGIC, pos))
            else:
                stack.append((utoken, TOKEN_OPER, pos))
        elif token_type == TOKEN_PARENTHESIS and token == "(":
            _merge(output, scache, pos)
            stack.append((token, TOKEN_PARENTHESIS, pos))
        elif token_type == TOKEN_PARENTHESIS and token == ")":
            _merge(output, scache, pos)
            del scache[:]
            try:
                while not (stack[-1][1] == TOKEN_PARENTHESIS
                           and stack[-1][0] == "("):
                    output.append(stack.pop())
            except IndexError:
                raise ParseError(
                    "parenthesis mismatch at position %s" % (pos))
            stack.pop()
        else:
            scache.append(token)

    _merge(output, scache, pos)

    if stack and stack[-1][0] == "(":
        raise ParseError(
            "parenthesis mismatch at position %s" % output[2])

    while stack:
        output.append(stack.pop())

    return output

if __name__ == '__main__':
    c = 'dns_ip ~ 10.232 or (dns_ip ~ 10.254. and manifest in rack_server)'
    #c = "k ^ 'count(0)' AND (m == 'AND'   and a=='' and b==' ') and c===3 and d!==4 and f!=5 and g==8 and h~9 and i!~10 and j<11 and k<=12 and l>13 and m>=14 and (n)"
    #c = "id in 1,2,3,4,5"
    print lex(c)
    print parse(c)
