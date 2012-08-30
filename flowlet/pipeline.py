import sys
from greenlet import greenlet, GreenletExit
from types import XRangeType, GeneratorType, DictionaryType
from functools import wraps
from itertools import islice, count, cycle, chain
from collections import Iterable, deque

is_pypy = hasattr(sys, 'pypy_version_info')
is_cpython = not is_pypy

if is_cpython:
    from flow import Id
else:
    Id = lambda x: x

isiterator  = lambda obj: isinstance(obj, Iterable)
isgenerator = lambda obj: isinstance(obj, GeneratorType)
isgreenlet  = lambda obj: isinstance(obj, greenlet)
iscoroutine = lambda obj: isgreenlet(obj) or (isgenerator(obj) and hasattr(obj, 'send'))

coercions = {
    # Internals
    XRangeType     : lambda obj: LazyPipe(source=obj),
    DictionaryType : lambda obj: StrictPipe(source=obj.iteritems()),
    GeneratorType  : lambda obj: LazyPipe(source=obj),
    set            : lambda obj: StrictPipe(source=iter(obj)),
    tuple          : lambda obj: StrictPipe(source=iter(obj)),
    list           : lambda obj: StrictPipe(source=iter(obj)),

    # Itertools
    count          : lambda obj: LazyPipe(source=iter(obj)),
    islice         : lambda obj: LazyPipe(source=iter(obj)),
    cycle          : lambda obj: LazyPipe(source=iter(obj))
}

class BlockedUpstream(Exception):
    pass

# This is a sentinel, it should be discarded by the logic, if not
# then it will raise BlockedUpstream since it corrsponds to a
# broken pipe. It is passed purely to kickstart the generator.
class Nothing(object):

    def __iter__(self):
        raise BlockedUpstream

    def __nonzero__(self):
        return False

def pipe_coerce(obj):
    """
    Upcast a Python type into the Slipstream universe
    """
    mapping = coercions.get(type(obj))

    if mapping:
        return mapping(obj)
    else:
        raise CannotCoerce(obj)

class CannotCoerce(Exception):

    def __init__(self, obj):
        self.unknown_type = type(obj)

    def __str__(self):
        return "Cannot coerce type (%r) into Pipe instance" % ( self.unknown_type)

def lazy(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return LazyPipe(logic=f, args=args, kwargs=kwargs, name=f.__name__)
    return wrapper

def strict(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return StrictPipe(logic=f, args=args, kwargs=kwargs, name=f.__name__)
    return wrapper

# Base Pipe
# =========

class Pipe(object):

    def __init__(self, source=None, logic=None, args=None,
            kwargs=None, name=None, composite=False):

        self.started = False
        self.finalized = False

        # When you compose two pipes, the internal logic of the pipes
        # become fused and the original pipes disappear. You cannot
        # recover the original pipes from the composite.

        self.composite = composite

        # Reference to the upstream pipe, set at bind-time
        self.up = None
        self.logic = logic

        self.args = args or ()
        self.kwargs = kwargs or {}
        self.name = name or self.__class__.__name__

        self.line = []

        if source:
            self.logic = lambda _: iter(source)
        else:
            self.logic = logic

    @staticmethod
    def bind(A, B):
        if A.composite and B.composite:
            f = A.logic
            g = B.logic
            co = lambda x,y: g(x, f(Id, y))

        elif A.composite:
            f = A.logic
            co = lambda x,y: x(f(B, y))

        elif B.composite:
            f = B.logic
            co = lambda x,y: f(x, A(y))

        else:
            co = lambda x,y: x(B(A(y)))

        return Pipe(
            logic=co, name='(%s.%s)' % (A.name, B.name),
            composite=True
        )

    # XXX: deprecated??
    def __or__(self, dstruct):
        """ Deconstructor """
        assert callable(dstruct)

        if self.composite:
            return self.logic(dstruct, [])
        else:
            return dstruct(self.logic([]))

    def __rshift__(self, other):
        """ Vertical composition """

        if not isinstance(other, Pipe):
            other = pipe_coerce(other)

        return self.bind(self, other)

    def __rrshift__(self, other):

        if not isinstance(other, Pipe):
            other = pipe_coerce(other)

        return self.bind(other, self)

    def __repr__(self):
        if hasattr(self, 'it'):
            return 'Pipe %s (%s)' % (self.name, repr(self.it))
        else:
            return 'Pipe %s ()' % (self.name)

    __lshift__ = __rrshift__
    __rlshift__ = __rshift__

    # Serialize to disk
    #__getinitargs__ = __init__
    #__getnewargs__ = object.__new__
    #__getstate__ = __dict__
    __deepcopy__ = None

# Lazy Pipe
# =========

class LazyPipe(Pipe):
    """
    A lazy pipe.
    """
    lazy = True

    def __call__(self, ins):
        return self.logic(ins, *self.args, **self.kwargs)

# Strict Pipe
# ===========

class StrictPipe(Pipe):
    """
    A strict pipe.
    """
    lazy = False
    bounded = False

    def force(self, stream):
        # Can be overloaded with a more efficient implementation.
        if self.bounded:
            assert hasattr(self, 'maxsize'),\
            "Specified bounded but no maxsize specified"
            return iter(deque(stream, maxlen=self.maxsize))
        else:
            return iter(deque(stream))

    def __call__(self, ins):
        return self.force(self.logic(ins, *self.args, **self.kwargs))

def extract(ins):
    result = list(ins)
    if hasattr(ins, 'close'):
        ins.close()
    return result

def runPipeline(line, dstruct=list):
    if hasattr(line, 'composite') and line.composite:
        result = line.logic(dstruct, Nothing())
    else:
        result = dstruct(line.logic(Nothing()))
    return result

def iterPipeline(line):
    return runPipeline(line, Id)
