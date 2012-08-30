from __future__ import print_function

from functools import partial
from itertools import islice, count, chain

from Queue import Empty
from contextlib import closing
from flow import await, send, close, BlockedUpstream, flowlet as _flowlet
from flow import Id, exhaust
from multiprocessing import Process, Queue
from select import select

from flowlet import flowlet, Flowlet
from pipeline import lazy, strict, runPipeline

try:
    import cython
    have_cython = True
except:
    have_cython = False

try:
    import numexpr
    have_numexpr = True
except:
    have_numexpr = False

idStrict = strict(Id)
idLazy   = lazy(Id)

# Core
# ----

# pipe :: (a -> b) -> a ~> b
@flowlet
def pipe(f):
    while 1:
        x = await()
        send(f(x))

# pipe_ :: (a -> b) -> a ~> ()
@flowlet
def pipe_(f):
    while 1:
        x = await()
        f(x)

# printer_ :: (a -> b) -> a ~> ()
printer_ = lambda: pipe_(print)

# printer :: (a -> b) -> a ~> b
printer  = lambda: pipe(print)

# forever :: (a -> b) -> a ~> b
@flowlet
def forever(f):
    while 1:
        send( f() )

# take :: n -> (a ~> b)
@lazy
def take(iterator, n):
    """
    Int -> Pipe ( a -> b )
    """
    return islice(iterator, n)

# collect :: n -> (a ~> [a])
@flowlet
def collect(n):
    accum = []
    i = 0
    while i < n:
        accum.append(await())
        i += 1
    close()
    send(accum)

# consume :: (a ~> ())
@strict
def consume(iterator):
    exhaust(iterator)
    return ()

# repeat :: (a -> b) -> a ~> b
def repeat(f):
    while True:
        yield f()

# forM :: (a -> b) -> n -> a ~> b
@flowlet
def forM(f, n=0):
    for i in count(n):
        send( f(i) )

@lazy
def flatten(iterator):
    return chain.from_iterable(iterator)

# Resources
# ---------

@flowlet
def filepipe(fname):
    with open(fname) as f:
        while 1:
            send(f.readline())

@flowlet
def queuepipe(queue, block=True):
    while 1:
        try:
            item = queue.get(block=block)
            send(item)
        except (Empty, EOFError):
            close()
            break

@flowlet
def queueput(queue):
    while 1:
        try:
            x = await()
            queue.put(x)
        except BlockedUpstream:
            close()
            break

@flowlet
def ipcpipe(pipe):
    with closing(pipe):
        while 1:
            try:
                item = pipe.recv()
                send(item)
            except EOFError:
                close()
                break

@flowlet
def sockpipe(sock):
    with closing(sock):
        while 1:
            try:
                item = sock.recv()
                send(item)
            except EOFError:
                close()
                break

# barrier :: (a -> Bool) ~> (a ~> b)
@flowlet
def barrier(f):
    locked = True

    while 1:
        x = await()
        if f(x):
            locked = False
        if not locked:
            send(x)

# filter :: (a -> Bool) ~> (a ~> b)
@flowlet
def filter(f):
    x = await()
    if f(x):
        send(x)

# first :: (() ~> a) -> (x ~> (a, x))
def first(f, *args, **kwargs):
    def composite(f2):
        wrap = _flowlet(f2)
        for i in wrap:
            send(i, await())
    return Flowlet(logic=composite, args=(f,), kwargs=kwargs)

# second :: (() ~> a) -> (x ~> (a, x))
def second(f, *args, **kwargs):
    def composite(f2):
        wrap = _flowlet(f2)
        for i in wrap:
            send(await(), i)
    return Flowlet(logic=composite, args=(f,), kwargs=kwargs)

# split :: a ~> (a,a)
@flowlet
def split():
    while 1:
        x = await()
        send((x,x))

# unsplit :: (a -> b -> c) -> (a,b) ~> c
@flowlet
def unsplit(f):
    while 1:
        x,y = await()
        send(f(x,y))

# dimap :: (a -> b) -> (a -> c) -> (a ~> (b,c))
@flowlet
def dimap(f,g):
    while 1:
        x = await()
        send((f(x), g(x)))

# parmap :: (a -> b) -> (c -> d) -> ((a,b) ~> (c,d))
@flowlet
def parmap(f,g):
    while 1:
        x,y = await()
        send((f(x), g(y)))

# ``scatter`` will take a container, split it into equal parts
# *atemporally* and distribute to workers. Forces the entire stream into
# memory at the call site.

# ``roundrobin`` will take a container, split it into equal parts
# *across time* and distribute to workers. Does not force the stream.

@strict
def scatter(it, N=1):
    # Write this in terms of repeated calls to islice(it, n)
    m = list(it)
    for i in xrange(0, len(m), N):
        idx = i % N
        yield (((idx, m[i:i+N])))

@lazy
def roundrobin(it, n):
    for i, e in enumerate(it):
        yield (i, e)

def pctx(qi, l, qo):
    pline = queuepipe(qi) >> l >> queueput(qo)
    return partial(runPipeline, pline)

@flowlet
def par(*lines, **kw):
    NS = kw.get('N')
    N  = kw.get('N') or len(lines)
    if NS: lines = lines*N

    qi = [Queue() for _ in xrange(N)]
    qo = [Queue() for _ in xrange(N)]
    ps = []

    # suspend
    send(qo)
    # resume

    for i in xrange(N):
        parline = pctx(qi[i], lines[i], qo[i])
        p = Process(target=parline)
        p.start()
        ps.append(p)

    while 1:
        # suspend
        ins = await()
        # resume

        if ins is not None:
            idx, it = ins
            qi[idx].put(it)

            # suspend
            send(idx)
            # resume
        else:
            for p in ps:
                p.terminate()
            close()
            break

def allgather(idx, qo):
    qs = await()
    rlist = [q._reader for q in qs]

    while 1:
        read, _, _ = select(rlist)
        for i in read:
            send(i.recv())

@flowlet
def gather():
    qo = await()
    while 1:
        idx = await()
        if idx is not None:
            send(qo[idx].get())
        else:
            close()
            break

# =================
# Numeric Pipelines
# =================

def cythonpipe(f):
    if not have_cython:
        raise RuntimeError("Cython is not installed")
    return flowlet(cython.compile(f))

@flowlet
def numexpr_pipe(f):
    if not have_numexpr:
        raise RuntimeError("Numexpr is not installed")

    vm = numexpr.NumExpr(f)
    while 1:
        inputs = await()
        outputs = vm(inputs)
        send(outputs)
