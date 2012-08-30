import _multiprocessing
import os
from itertools import count
import threading
from operator import add

from copy import copy
from mock import MagicMock
from nose.tools import assert_raises
from unittest2 import skip

from flowlet.flowlet import *
from flowlet.prelude import *
from flowlet.pipeline import *
from flowlet.flow import exhaust, await, send, Id

# For static resources, :-/
os.chdir(os.path.dirname(os.path.abspath( __file__)))

# ==================
# Iterator Utilities
# ==================

def test_id():
    x = object()
    assert Id(x) == x
    assert id(Id(x)) == id(x)
    assert Id(x) is x

def test_exhaust():
    # Assert it doesn't segfault!
    exhaust([1,2,3])

def test_exhaust2():

    def go(): yield

    g = go()
    exhaust(g)

    assert not g.gi_running

def test_exhaust_invalid():
    with assert_raises(TypeError):
        exhaust(1)

# ================
# Context Managers
# ================

def test_flowlet_finalize():

    flag = []

    class resource(object):

        def __init__(self, flag):
            self.flag = flag

        def __enter__(self):
            pass

        def __exit__(self, exc_type, exc_value, traceback):
            self.flag.append(True)

    @flowlet
    def M(flag):
        with resource(flag):
            x = await()
            y = await()
            send(x+y)

    a = LazyPipe(source=[2,2])
    line = a >> M(flag) >> collect(2)

    result = runPipeline(line)
    assert flag == [True]

# ======================
# Decorator Constructors
# ======================

def test_pipe_lazy_decorator1():

    @lazy
    def lpipe(input):
        for i in input:
            yield i+1

    pipe = LazyPipe(source=count(0)) >> lpipe() >> take(2)
    result = runPipeline(pipe)

    assert result == [1,2]

def test_pipe_lazy_decorator2():

    @lazy
    def lpipe(input):
        return islice(input, 3)

    pipe = LazyPipe(source=count(0)) >> lpipe()
    result = runPipeline(pipe)

    assert result == [0,1,2]

def test_pipe_strict_decorator():

    @strict
    def rpipe(input):
        for i in input:
            yield i+1

    pipe = [0,1,2] >> rpipe()
    result = runPipeline(pipe)

    assert result == [1,2,3], repr(result)

# ========
# Flowlets
# ========

def test_flowlet1():

    @flowlet
    def M():
        while True:
            x = await()
            y = await()
            send(3)

    a = LazyPipe(source=[1,2])
    line = a >> M()

    result = runPipeline(line)
    assert result == [3]

def test_flowlet2():

    @flowlet
    def M():
        while True:
            x = await()
            y = await()
            send(x+y)

    a = LazyPipe(source=[1,2,3,4])
    line = a >> M() >> take(2)

    result = runPipeline(line)
    assert result == [3, 7]

def test_flowlet3():

    @flowlet
    def M():
        while True:
            x = await()
            send(x)

    a = LazyPipe(source=[1,2,3,4])
    line = a >> M()

    result = runPipeline(line)
    assert result == [1,2,3,4]

def test_flowlet4():

    @flowlet
    def M():
        while True:
            x = await()
            send(x)

    a = LazyPipe(source=[1,2,3,4])
    c = take(3)

    line = a >> M() >> c

    result = runPipeline(line)
    assert result == [1,2,3]

def test_flowlet5():

    @flowlet
    def M():
        pass

    @flowlet
    def N():
        await()
        await()
        await()

    a = StrictPipe(source=[1,2,3])
    line = a >> M() >> N()

    result = runPipeline(line)
    assert result == []

def test_flowlet6():

    @flowlet
    def M():
        while True:
            x = await()
            send(x)
            y = await()

    line = [1,2] >> M()

    result = runPipeline(line)
    assert result == [1], repr(result)

def test_flowlet7():

    @flowlet
    def M():
        await()
        raise Exception("Boom")

    line = [] >> M()

    result = runPipeline(line)
    assert result == [], repr(result)

def test_flowlet8():

    @flowlet
    def M():
        await()
        send(1)
        send(2)

    line = [] >> M() >> take(2)

    result = runPipeline(line)
    assert result == [], repr(result)


def test_flowlet_source():

    @flowlet
    def counting(n):
        for i in xrange(n):
            send(i)

    a = counting(5)
    b = take(5)

    result = runPipeline(a >> b)
    assert result == range(5)

def test_flowlet_source2():

    @flowlet
    def counting():
        send(1)
        send(2)
        send(3)

    a = counting()
    b = take(25)

    result = runPipeline( a >> b )
    assert result == [1,2,3]

def test_flowlet_combine():

    @flowlet
    def combine():
        while True:
            x = await()
            y = await()
            send((x,y))

    a = [1,2,3,4]
    b = combine()
    c = take(25)

    result = runPipeline(a >> b >> c)
    assert result == [(1,2), (3,4)]

# =============
# Associativity
# =============

def test_compose_segments():

    a = count(0)
    b = take(3)

    segment  = a >> b
    segment2 = segment >> take(2)

    result = runPipeline(segment2)
    assert result == [0,1]

def test_compose_segments2():


    a = count(0)
    b = pipe(lambda x:x)
    c = idLazy()
    d = take(3)

    segment  = b >> c
    segment2 = a >> ( segment )>> d

    result = runPipeline(segment2)
    assert result == [0,1,2]

def test_compose_associativity():

    a = count(0)
    b = pipe(lambda x: x+1)
    c = pipe(lambda x: x*2)
    d = take(3)

    result = runPipeline(a >> b >> c >> d)

    a = count(0)
    b = pipe(lambda x: x+1)
    c = pipe(lambda x: x*2)
    d = take(3)

    result2 = runPipeline(((a >> b) >> c) >> d)

    assert result == result2

def test_compose_associativity2():

    a = [1,2,3]
    b = pipe(lambda x: x+1)
    c = pipe(lambda x: x%2)
    d = take(3)

    result1 = runPipeline(a >> b >> c >> d)
    result2 = runPipeline(a >> (b >> (c >> d)))
    result3 = runPipeline(a >> (b >> c) >> d)

    assert result1 == result2 == result3

def test_compose_reverse_associativity():

    a = count(0)
    b = pipe(lambda x: x+1)
    c = pipe(lambda x: x*2)
    d = take(3)

    result = runPipeline(d << c << b << a)

    a = count(0)
    b = pipe(lambda x: x+1)
    c = pipe(lambda x: x*2)
    d = take(3)

    result2 = runPipeline(((d << c) << b) << a)

    assert result == result2

def test_robust_associativity():

    def foo(ins):
        for i in ins:
            yield i+1

    def bar(ins):
        for i in ins:
            yield i%2

    def take(ins):
        for i in xrange(5):
            yield ins.next()

    def gen(ins):
        return cycle([1,2,3])

    a = LazyPipe(name='a', logic=gen)
    b = LazyPipe(name='b', logic=foo)
    c = LazyPipe(name='c', logic=bar)
    d = LazyPipe(name='d', logic=take)

    line = ((a >> b) >> c) >> d
    r1 = runPipeline(line)

    line = a >> (b >> (c >> d))
    r2 = runPipeline(line)

    line = a >> b >> (c >> d)
    r3 = runPipeline(line)

    line = a >> (b >> c) >> d
    r4 = runPipeline(line)

    line = (a >> b) >> c >> d
    r5 = runPipeline(line)

    line = a >> b >> c >> d
    r6 = runPipeline(line)

    assert r1 == r2 == r3 == r4 == r5 == r6

def test_robust_associativity2():

    a = [1,2,3,4,5]
    b = pipe(lambda x:x+1)
    c = pipe(lambda x:x%2)
    d = pipe(lambda x:x*4)
    e = pipe(lambda x:x-1)
    f = take(5)

    line = (a >> b >> c >> d >> e >> f)
    r1 = runPipeline(line)

    line = (a >> b) >> (c >> d) >> (e >> f)
    r2 = runPipeline(line)

    assert r1 == r2

def test_robust_associativity3():

    a = [1,2,3,4,5]
    b = pipe(lambda x:x+1)
    c = pipe(lambda x:x%2)
    d = pipe(lambda x:x*4)
    e = pipe(lambda x:x-1)
    f = take(5)

    line = (a >> b >> c >> d >> e >> f)
    r1 = runPipeline(line)

    line = ((a >> b) >> (c >> d)) >> (e >> f)
    r2 = runPipeline(line)

    assert r1 == r2

# ======
# Purity
# ======

def test_pipeline_immutable():
    # Running the elements should not alter the state of the
    # elements in the pipeline
    a = LazyPipe(source=count(1))
    b = take(3)

    s1 = copy(a).__reduce__()[2]
    s2 = copy(b).__reduce__()[2]

    line =  a >> b

    # vestigal
    assert line != a
    assert line != b
    assert line.composite

    runPipeline(a >> b)

    assert s1 == a.__reduce__()[2]
    assert s2 == b.__reduce__()[2]

# ===========
# runPipeline
# ===========

def test_single_runpipeline():
    a = LazyPipe(source=[1])

    result = runPipeline(a)
    assert result == [1]

def test_runpipeline():

    def bomb():
        raise Exception("Boom")
        yield

    a = LazyPipe(source=bomb())

    with assert_raises(Exception):
        runPipeline(a)

# ============
# Initializers
# ============

def test_strict_start():

    def bomb():
        raise RuntimeError("Boom")
        yield

    # Assert lazy consumption doesn't raise the exception
    a = StrictPipe(source=bomb())

    with assert_raises(RuntimeError):
        runPipeline(a)

def test_lazy_start():

    def bomb():
        raise RuntimeError("Boom")
        yield

    # Assert lazy consumption doesn't raise the exception
    a = LazyPipe(source=bomb())

    with assert_raises(RuntimeError):
        runPipeline(a)

# ==========
# Finalizers
# ==========

def test_pipeline_close():

    # Assert lazy consumption doesn't raise the exception
    gr = (a for a in xrange(25))

    a = LazyPipe(source=gr)
    b = pipe(lambda x: x+1)

    line = a >> b
    runPipeline(line)

    assert gr.gi_running == False

# =========
# Pipelines
# =========

def test_simple_pipeline1():
    a = LazyPipe(source=[1,2,3,4])
    result = runPipeline(a)
    assert result == [1,2,3,4]

def test_simple_pipeline2():
    a = LazyPipe(source=[1,2,3,4])
    b = pipe(lambda x: x**2)

    result = runPipeline(a >> b)
    assert result == [1,4,9,16]

def test_simple_pipeline3():
    a = LazyPipe(source=[1,2,3,4])
    b = pipe(lambda x: x**2)
    c = pipe(lambda x: x+1)

    result = runPipeline(a >> b >> c)
    assert result == [2,5,10,17]

def test_simple_pipeline2_reverse():
    a = LazyPipe(source=[1,2,3,4])
    b = pipe(lambda x: x**2)

    result = runPipeline(b << a)
    assert result == [1,4,9,16]

def test_simple_pipeline3_reverse():
    a = LazyPipe(source=[1,2,3,4])
    b = pipe(lambda x: x**2)
    c = pipe(lambda x: x+1)

    result = runPipeline(c << (b << a))
    assert result == [2,5,10,17], repr(result)

# ========
# Laziness
# ========

def test_lazy_source():
    # Consume an infinite data stream as a source, assert that we
    # only push a finite portion of it as needed

    a = LazyPipe(source=count(1))
    it = runPipeline(a, Id)

    assert it.next() == 1
    assert it.next() == 2

def test_lazy_identity():
    # Consume an infinite data stream as a source, assert that we
    # only push a finite portion of it as needed

    a = LazyPipe(source=[1,2,3])
    b = idLazy()

    result = runPipeline(a >> b)
    assert result == [1,2,3]

def test_lazy_identity_reverse():

    a = idLazy()
    b = LazyPipe(source=[1,2,3])

    result = runPipeline(a >> b)
    assert result == [1,2,3]

# ==========
# Strictness
# ==========

def test_strict_identity():

    a = LazyPipe(source=[1,2,3])
    b = idStrict()

    result = runPipeline(a >> b)
    assert result == [1,2,3]

def test_lazy_to_strict():

    a = LazyPipe(source=[1,2,3])
    b = idStrict()

    result = runPipeline(a >> b)
    assert result == [1,2,3]

def test_strict_bonded():

    a = LazyPipe(source=xrange(10))
    b = idStrict()
    b.bounded = True
    b.maxsize = 5

    result = runPipeline(a >> b)
    assert result == [5,6,7,8,9]

# =============
# Decomposition
# =============

def test_decompose1():

    a = LazyPipe(source=[1,2,3,4])

    result = a | tuple

    assert result == (1,2,3,4)

def test_decompose2():

    a = LazyPipe(source=[1,2,3,4])

    result = a | list

    assert result == [1,2,3,4]

def test_decompose3():

    a = StrictPipe(source=[1,2,3,4])

    result = a | list

    assert result == [1,2,3,4]

# ====
# pipe
# ====

def test_pipe():
    a = LazyPipe(source=[1,2,3,4])
    b = pipe(lambda x: x+1)

    result = runPipeline(a >> b)
    assert result == [2,3,4,5]

def test_pipe_():
    a = LazyPipe(source=[1,2,3])
    b = pipe_(lambda x: 'not used')

    result = runPipeline(a >> b)
    assert result == []

def test_lazy_pipe():
    a = LazyPipe(source=count(1))
    b = pipe(lambda x: x+1)

    line = a >> b
    it = iterPipeline(line)

    assert it.next() == 2
    assert it.next() == 3
    assert it.next() == 4

# =======
# consume
# =======

def test_consume():
    a = (a for a in xrange(3))
    b = consume()

    pipe = a >> b
    runPipeline(pipe)

    assert a.gi_running == False

# =======
# collect
# =======

def test_collect():
    a = [1,2,3,4,5]
    b = collect(5)

    pipe = a >> b
    result = runPipeline( pipe )
    assert result == [a]

# ====
# take
# ====

def test_take():
    a = count(0)
    b = take(3)

    pipe = a >> b
    result = runPipeline( pipe )
    assert result == [0,1,2]

def test_take_repeated():
    a = [None, 'foo', 1, 2.718]
    b = take(len(a) * 3)

    pipe = (a*4) >> b
    result = runPipeline( pipe )

    assert result == (a*3)

def test_take_refine1():
    a = count(0)
    b = take(3)
    c = take(2)
    d = take(1)

    pipe = a >> b >> c >> d
    result = runPipeline(pipe)

    assert result == [0]

def test_take_refine2():
    a = count(0)
    b = take(5)
    c = take(4)
    d = take(3)

    pipe = a >> b >> c >> d
    result = runPipeline(pipe)

    assert result == [0,1,2]

def test_lazy_take():
    a = LazyPipe(source=count(1))
    b = take(3)

    pipe = a >> b
    pipe = iterPipeline(pipe)

    assert not a.finalized
    assert not b.finalized

    assert pipe.next() == 1
    assert pipe.next() == 2
    assert pipe.next() == 3

    with assert_raises(StopIteration):
        pipe.next()

def test_strict_take():
    a = StrictPipe(source=[1,2,3])
    b = take(3)

    pipe = a >> b
    result = iterPipeline(pipe)

    assert result.next() == 1
    assert result.next() == 2
    assert result.next() == 3

    with assert_raises(StopIteration):
        result.next()

def test_pipeline_take_less():
    a = LazyPipe(source=count(1))
    b = take(3)

    result = runPipeline(a >> b)
    assert len(result) == 3
    assert result == [1,2,3]

def test_pipeline_take_more():
    a = LazyPipe(source=[1,2,3])
    b = take(25)
    c = take(50)

    result = runPipeline(a >> b >> c)
    assert len(result) == 3
    assert result == [1,2,3]

def test_pipeline_consume():
    a = LazyPipe(source=[1,2,3])
    b = consume()

    result = runPipeline(a >> b)
    assert len(result) == 0

# =======
# repeat
# =======

def test_repeat():
    a = repeat(lambda: None)
    b = take(3)

    result = runPipeline(a >> b)
    assert result == [None, None, None]

# ====
# forM
# ====

def test_for():
    a = forM(lambda x: x**2)
    b = take(3)

    result = runPipeline(a >> b)
    assert result == [0, 1, 4]

# =======
# forever
# =======

def test_forever():
    a = forever(lambda: 'fizzbar')
    b = take(2)

    result = runPipeline(a >> b)
    assert result == ['fizzbar', 'fizzbar']

# =======
# barrier
# =======

def test_barrier():
    line = [False, 'foo', True, 2, 3] >> barrier(lambda x: x == True)

    result = runPipeline(line)
    assert result == [True, 2, 3]

# =======
# flatten
# =======

def test_flatten_lists():
    a = [[1,2], [3,4]]
    b = flatten()

    result = runPipeline(a >> b)
    assert result == [1,2,3,4]

def test_flatten_generator():
    a = (xrange(0,n) for n in xrange(4))
    b = flatten()

    result = runPipeline(a >> b)
    assert result == [0,0,1,0,1,2]

# ====
# Bind
# ====

def test_binds_lazy():
    a = LazyPipe(source=[1,2,3])
    b = LazyPipe(source=[1,2,3])

    bound =  LazyPipe.bind(a, b)
    assert isinstance(bound, Pipe)


def test_binds_Strict():
    a = LazyPipe(source=[1,2,3])
    b = LazyPipe(source=[1,2,3])

    bound =  StrictPipe.bind(a, b)
    assert isinstance(bound, Pipe)

def test_invalid_binds():
    a = LazyPipe(source=[1,2,3])
    b = LazyPipe(source=[1,2,3])

    LazyPipe.bind(a, b)
    StrictPipe.bind(a, b)

# =========
# Coercions
# =========

def test_no_coerce():
    with assert_raises(CannotCoerce):
        runPipeline(object() >> take(2))

def test_coerce_xrange():
    result = runPipeline(xrange(5) >> take(2))
    assert result == [0,1]

def test_coerce_generator():

    def gen():
        yield 1
        yield 2

    runPipeline(gen() >> take(2))

def test_coerce_generator_norun():

    def bomb():
        raise Exception("Boom")

    def gen():
        yield bomb()

    # does not blow up
    runPipeline(gen() >> take(0))

    # does blow up
    with assert_raises(Exception):
        runPipeline(gen() >> take(1))

def test_coerce_list():

    a = [1,2,3]
    b = take(2)

    pipe = a >> b
    result = runPipeline(pipe)
    assert result == [1,2]

def test_coerce_dictionary():

    a = {'a': 2}
    b = take(2)

    pipe = a >> b
    result = runPipeline(pipe)
    assert result == [('a',2)]

def test_coerce_set():

    a = {'a', 'b', 'c'}
    b = take(3)

    pipe = a >> b
    result = runPipeline(pipe)
    assert set(result) == a

def test_coerce_count():
    result = runPipeline(count(0) >> take(2))
    assert result == [0,1]

def test_coerce_cycle():
    result = runPipeline(cycle([1,2]) >> take(4))
    assert result == [1,2,1,2]

# ========
# Blocking
# ========

def test_coerce_blocked():

    def gen():
        yield 1
        yield 2

    with assert_raises(BlockedUpstream):
        result = runPipeline(take(2) >> gen())
        assert result == []

def test_no_source_block():
    def gen():
        yield 1
        yield 2

    with assert_raises(BlockedUpstream):
        runPipeline(take(2) >> consume())

# =====
# Queue
# =====

def test_queue():
    from Queue import Queue
    q = Queue()

    q.put(1)
    q.put(2)
    q.put(3)

    a = queuepipe(q, block=False)
    b = take(5)

    result = runPipeline(a >> b)
    assert result == [1,2,3]

# =====
# Files
# =====

def test_file_open_needed():
    a = filepipe('example.txt')
    b = take(2)

    result = runPipeline(a >> b)
    assert result == ['this is line 1\n', 'this is line 2\n']

def test_file_open_notneeded():

    # The file is not opened unless needed. Would raise IOError
    # otherwise.
    a = filepipe('does_not_exit.txt')
    b = take(0)

    result = runPipeline(a >> b)
    assert result == []

# -------
# Flowlet
# -------

def test_flowlet_close():
    mock_open = MagicMock()

    @flowlet
    def closeTest():
        with mock_open:
            for i in xrange(25):
                await()

    a = closeTest()
    result = runPipeline([] >> a)

    assert mock_open.__exit__.called == True
    assert result == []

def test_flowlet_close2():
    mock_open = MagicMock()

    @flowlet
    def closeTest():
        with mock_open:
            send(1)
            send(2)
            send(3)

    a = closeTest()
    b = take(5)

    result = runPipeline(a >> b)

    assert mock_open.__exit__.called
    assert result == [1,2,3]

def test_flowlet_suite1():

    @flowlet
    def M():
        while True:
            x = await()
            y = await()
            z = await()
            send((x,y,z))

    line = [1,2,3,4,5,6] >> M()
    result = runPipeline(line)

    assert result == [(1,2,3), (4,5,6)]

def test_flowlet_suite2():

    @flowlet
    def M():
        x = await()
        send(x)
        y = await()
        send(y)

    line = [1,2] >> M()
    result = runPipeline(line)

    assert result == [1,2]

def test_flowlet_suite3():

    @flowlet
    def M():
        x = await()
        y = await()
        send(x)
        send(y)

    pipe = [1,2] >> M()
    result = runPipeline(pipe)

    assert result == [1,2]

def test_flowlet_suite4():

    @flowlet
    def M():
        while True:
            x = await()
            y = await()
            send((x,y))

    pipe = [1,2,3,4,5,6,7] >> M()
    result = runPipeline(pipe)

    assert result == [(1,2), (3,4), (5,6)]

def test_flowlet_suite5():

    @flowlet
    def M():
        send(1)
        send(2)
        send(3)

    pipe = M() >> collect(3)
    result = runPipeline(pipe)

    assert result == [[1,2,3]]

def test_flowlet_suite6():

    # Question should this even be allowed?
    # We used to know at init() whether we were immedietely
    # saturated, now we don't know that until we try an
    # operation.

    @flowlet
    def M():
        send('foo'),
        x = await()
        send('bar')
        y = await()
        send('awk')
        send(x+y)

    @flowlet
    def N():
        send(1)
        send(2)

    pipe = N() >> M()
    result = runPipeline(pipe)
    assert result == ['foo', 'bar', 'awk', 3]

def test_flowlet_suite7():

    @flowlet
    def M():
        send(1)
        send(2)
        await()
        await()
        send(3)

    pipe = [None,None] >> M()
    result = runPipeline(pipe)
    assert result == [1,2,3]

def test_flowlet_suite8():

    @flowlet
    def M():
        send(1)
        send(2)
        await()
        await()

    pipe = [1,2] >> M()
    result = runPipeline(pipe)

    assert result == [1,2]

def test_flowlet_suite9():

    @flowlet
    def M():
        send(1)
        send(2)
        send(3)
        x = await()
        send(x)


    pipe = xrange(1) >> M() >> take(4)
    result = runPipeline(pipe)

    assert result == [1,2,3,0]

def test_flowlet_suite10():

    @flowlet
    def M():
        x, y, z = (await(), await(), await())
        send((x,y,z))

    pipe = {'a','b','c'} >> M()
    result = runPipeline(pipe)

    assert result == [('a', 'c', 'b')]

def test_flowlet_suite11():

    @flowlet
    def M():
        map(send, range(5))

    pipe = M() >> take(5)
    result = runPipeline(pipe)

    assert result == range(5)

def test_flowlet_suite12():

    @flowlet
    def M():
        send('foo'),
        x = await()
        send(x)
        send('bar')
        y = await()
        send('awk')
        send(y)

    pipe = [1,2] >> M()
    result = runPipeline(pipe)
    assert result == ['foo', 1, 'bar', 'awk', 2]

def test_flowlet_suite13():

    @flowlet
    def M():
        x = await()
        send(x)
        y = await()
        send(y)
        y = await()
        send(y)

    pipe = [1,2,3] >> M()
    result = runPipeline(pipe)
    assert result == [1,2,3]

def test_flowlet_suite14():

    @flowlet
    def M():
        send([1,2,3,4][await():await()])

    pipe = [1,3] >> M()
    result = runPipeline(pipe)
    assert result == [[2,3]]

def test_flowlet_suite15():

    @flowlet
    def M():
        send((lambda x,y: (x+y)/await())(await(), await()))

    pipe = [1,3,2] >> M()
    result = runPipeline(pipe)
    assert result == [2]

# ============
# Higher Order
# ============

def test_first():

    @flowlet
    def M():
        send(1)
        send(2)
        send(3)

    def N():
        send('a')
        send('b')
        send('c')

    line = M() >> first(N)
    result = runPipeline(line)
    assert result == [('a',1), ('b',2,),('c',3)]

def test_second():

    @flowlet
    def M():
        send(1)
        send(2)
        send(3)

    def N():
        send('a')
        send('b')
        send('c')

    line = M() >> second(N)
    result = runPipeline(line)
    assert result == [(1,'a'), (2,'b'),(3,'c')]

def test_split():

    @flowlet
    def M():
        send(1)
        send(2)
        send(3)

    line = M() >> split()
    result = runPipeline(line)
    assert result == [(1,1), (2,2), (3,3)]

def test_unsplit():

    @flowlet
    def M():
        send(1)
        send(2)
        send(3)

    line = [(1,1), (2,2), (3,3)] >> unsplit(add)
    result = runPipeline(line)
    assert result == [2,4,6]

def test_dimap():

    f = lambda x: x**2
    g = lambda y: y+1

    @flowlet
    def M():
        send(1)
        send(2)
        send(3)

    line = [1,2,3] >> dimap(f, g)

    result = runPipeline(line)
    assert result == [(1, 2), (4, 3), (9, 4)]

def test_parmap1():

    f = lambda x: x**2
    g = lambda y: y+1

    @flowlet
    def M():
        send(1)
        send(2)
        send(3)

    line = [1,2,3] >> split() >> parmap(f, g)

    result = runPipeline(line)
    assert result == [(1, 2), (4, 3), (9, 4)]

def test_parmap2():

    f = lambda x: x**2
    g = lambda y: y+1

    @flowlet
    def M():
        send(1)
        send(2)
        send(3)

    line = [(1,2),(3,4),(5,6)] >> parmap(f, g)
    result = runPipeline(line)
    assert result == [(1, 3), (9, 5), (25, 7)]

def test_parmap_dimap():

    f = lambda x: x*2
    g = lambda y: y+1

    line1 = xrange(50) >> dimap(f,g)
    line2 = xrange(50) >> split() >> parmap(f,g)

    assert runPipeline(line1) == runPipeline(line2)

def test_dimap_unpslit():

    f = lambda x: x*2
    g = lambda y: y+1

    line = xrange(5) >> dimap(f,g) >> unsplit(add)

    result = runPipeline(line)
    assert result == [1, 4, 7, 10, 13]

# -----------------
# Numeric Pipelines
# -----------------

def test_numexprpipe():
    from numpy import array
    pipe = [1,2,3] >> numexpr_pipe('x+1')
    result = runPipeline(pipe)

    assert result == [array(2), array(3), array(4)]

def test_numexprpipe2():
    from numpy import array, transpose

    x = array([[1,0],[0,1]])
    y = array([[0,1],[1,0]])

    line = [x,y] >> numexpr_pipe('x+1') >> pipe(transpose)
    result = runPipeline(line)

    assert result[0].all()
    assert result[1].all()

if __name__ == '__main__':
    import sys
    import nose
    sys.path.append(os.getcwd())
    nose.runmodule(argv=[__file__,'-x','--pdb', '--pdb-failure'], exit=False)
