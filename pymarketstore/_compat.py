import sys


PY2 = sys.version_info[0] == 2


if PY2:
    from inspect import getargspec as getfullargspec
else:
    from inspect import getfullargspec
