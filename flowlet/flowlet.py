import sys
from greenlet import greenlet
from functools import wraps, partial
from pipeline import Pipe, BlockedUpstream, Nothing
from greenlet import GreenletExit

is_pypy = hasattr(sys, 'pypy_version_info')
is_cpython = not is_pypy

if is_cpython:
    from flow import flowlet as _flowlet, send
elif is_pypy:
    from flow_pypy import flowlet as _flowlet
else:
    raise ImportError("Your interpreter is not supported!")

def flowlet(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return Flowlet(logic=f, args=args, kwargs=kwargs, name=f.__name__)
    return wrapper

# XXX: Write this in C
def from_iter(it):
    for i in it:
        send(i)

class Flowlet(Pipe):

    lazy = True

    def __init__(self, source=None, logic=None, args=None,
            kwargs=None, name=None, composite=False):
        self.started = False
        self.finalized = False
        self.composite = composite

        self.args = args or ()
        self.kwargs = kwargs or {}
        self.name = name or self.__class__.__name__

        self.logic = logic
        self.line = []

    def __call__(self, ins):

        fl = _flowlet(self.logic, *self.args, **self.kwargs)
        if isinstance(ins, _flowlet):
            fl.bind(ins)
        else:
            ins = _flowlet(from_iter, ins)
            fl.bind(ins)
        return fl
