__all__ = ["match"]

import re


def match(p, s):
    m = re.match(p, s)
    return m and setattr(match, "found", m.groups()) == None
