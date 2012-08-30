import _multiprocessing
import os

from flowlet.flowlet import *
from flowlet.prelude import *
from flowlet.pipeline import *
from flowlet.flow import exhaust, await, send, Id

from unittest2 import skip

# =========
# Unix Pipe
# =========

def test_pipe_recv():
    fd1, fd2 = os.pipe()
    r = _multiprocessing.Connection(fd1, writable=False)
    w = _multiprocessing.Connection(fd2, readable=False)

    w.send('foo')
    w.send('bar')

    a = ipcpipe(r)
    b = take(2)

    result = runPipeline(a >> b)
    assert result == ['foo', 'bar']

def test_pipe_close():
    fd1, fd2 = os.pipe()
    r = _multiprocessing.Connection(fd1, writable=False)
    w = _multiprocessing.Connection(fd2, readable=False)

    w.send('foo')
    w.send('bar')
    w.close()

    @flowlet
    def consume():
        await()
        await()

    a = ipcpipe(r)
    b = consume()

    pipe = a >> b
    runPipeline(pipe)

    assert r.closed

# ===========
# Parallelism
# ===========

def test_scatter1():
    pipe = [1,2,3] >> scatter(1)

    result = runPipeline(pipe)
    assert result == [(0,[1]), (0,[2]), (0,[3])]

def test_scatter2():
    pipe = [1,2,3] >> scatter(3)

    result = runPipeline(pipe)
    assert result == [(0,[1,2,3])]

def test_roundrobin():
    pipe = [1,2,3,4] >> roundrobin(2)

    result = runPipeline(pipe)
    assert result == [(0,1), (1,2), (2,3), (3,4)]

def test_parallel():

    task1 = pipe(lambda x: x*1)
    task2 = pipe(lambda y: y*2)

    line = (
        [1,2,3,4]
        >> scatter() >> par(task1, task2)
        >> gather()
    )
    result = runPipeline(line)

    assert result[0] != None
    assert sum(map(len,result)) == 4

def test_parallel2():

    task1 = idLazy()

    line = (
        [1,2,3,4]
        >> scatter(2) >> par(task1, N=5)
        >> gather()
        >> take(4)
    )
    result = runPipeline(line)

    assert result[0] != None
    assert sum(map(len,result)) == 4

@skip
def test_queued_endpoints():

    X = 'foo'
    Y = 'bar'

    qi = Queue()
    qo = Queue()

    qi.put(X)
    qi.put(Y)

    task1 = idLazy()
    parline = pctx(qi, task1, qo)

    t = threading.Thread(target=parline)
    t.start()

    x = qo.get()
    y = qo.get()

    try:
        assert x == X, x
        assert y == Y, y
    finally:
        t._Thread__stop()

def test_gather():

    N = 1
    task1 = pipe(lambda x: x)

    line = [1,2,3,4] >> scatter(N) >> par(task1,N=N) >> gather()
    #line = [1,2,3,4] >> scatter(1) >> par(task1) >> gatherer(gather)

    result = runPipeline(line)
    assert result == [[1], [2], [3], [4]]
