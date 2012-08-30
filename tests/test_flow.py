from gc import get_referents, collect

from flowlet.flow import flowlet, getcurrent, await, send, suspend, \
    close, FlowletExit, BlockedUpstream
from nose.tools import assert_raises
from unittest2 import skip
from weakref import ref

@skip
def test_gc():
    a = flowlet(lambda: None)

    assert len(get_referents(a.greenlet)) == 3
    b = ref(a.greenlet)
    del a
    collect()
    assert len(get_referents(b)) == 0

    # leakage = []

    # gc.collect(2)
    # before  = set(id(o) for o in gc.get_objects())

    # for i in f():
    #     pass

    # gc.collect(2)
    # after = gc.get_objects()

    # for x in after:
    #     if x is not before and id(x) not in before:
    #         leakage.append(x)


def test_construct():

    def M(x,y):
        return x+y

    fl = flowlet(M, 1, 2)
    assert fl.args == (1,2)
    assert fl.kwargs == {}
    assert fl.switch() == 3

def test_construct2():

    def M(x,y,a=None,b=None):
        return sum([x,y,a,b])

    fl = flowlet(M, 1, 2, a=3, b=4)

    assert fl.args == (1,2)
    assert fl.kwargs == {'a': 3, 'b': 4}
    assert fl.switch() == 10

@skip
def test_saturated():
    a = flowlet(lambda: None)
    assert not a.saturated

@skip
def test_greenlet_linking():
    def M():
        a = getcurrent()

    f = flowlet(M)
    f.greenlet.switch()

@skip
def test_greenlet_linking2():
    def M():
        send(1)
        assert await() == 2

    f = flowlet(M)
    rval = f.switch()
    assert rval == 1

def test_greenlet_linking3():
    def M():
        assert await() == 1
        x = send(2)

        assert getcurrent()

    f = flowlet(M)
    rval = f.switch(1)
    assert rval == 2

    assert getcurrent() == None

def test_flowlet_send():
    def M():
        assert await() == 1

    f = flowlet(M)
    f.send(1)

def test_flowlet_exception():
    def M():
        assert await() == 2

    f = flowlet(M)
    with assert_raises(AssertionError):
        f.switch(1)

def test_flowlet_await():
    def M():
        send(1)
        send(2)
        send(3)

    f = flowlet(M)
    assert f.args == ()
    assert f.kwargs == {}

    assert f.await() == 1
    assert f.await() == 2
    assert f.await() == 3

def test_flowlet_init():
    def M(x):
        send(x)

    f = flowlet(M)
    assert f.switch(1) == 1


def test_flowlet_init_args():
    def M(n):
        for i in xrange(n):
            send(i)

    f = flowlet(M, 3)
    assert f.await() == 0
    assert f.await() == 1
    assert f.await() == 2

def test_flowlet_init2():
    def M():
        x = await()
        send(1)
        send(2)
        send(3)

    f = flowlet(M)
    f.send('foo')
    assert f.await() == 1
    assert f.await() == 2
    assert f.await() == 3

def test_flowlet_init3():
    def M(x):
        send(x)

    f = flowlet(M)
    x = object()
    assert f.switch(x) == x

@skip
def test_invalid_schedule():
    def M():
        send(await())
        send(await())

    f = flowlet(M)
    x0 = 1

    with assert_raises(ValueError):
        f.await()
        f.send(x0)

def test_iter_sentinel():

    def M():
        send(1)
        send(2)
        send(3)

    f = flowlet(M)
    result = list(iter(f.await, None))
    assert result == [1,2,3]

def test_final():

    flag = [0]

    class resource(object):

        def __init__(self, flag):
            self.flag = flag

        def __enter__(self):
            pass

        def __exit__(self, exc_type, exc_value, traceback):
            # twiddle the flag
            self.flag[0] = 1
            pass

    def M(flag):
        with resource(flag):
            send(1)
            send(2)
            send(3)

    f = flowlet(M, flag)
    f.await()
    f.final()

    assert not f.active
    assert flag[0]

@skip
def test_suspend():

    def M():
        suspend()
        x = await()
        y = await()
        z = await()
        suspend()
        send(x)
        send(y)
        suspend()

    f = flowlet(M)
    f.resume()
    f.send(1)
    f.send(2)
    f.send(3)
    f.resume()
    assert f.await() == 1
    assert f.await() == 2
    f.resume()

@skip
def test_suspend2():

    def M():
        suspend()

    f = flowlet(M)

    with assert_raises(ValueError):
        f.send(1)

    f.resume()

    with assert_raises(ValueError):
        f.resume()

def test_schedule3():

    def M():
        x = await()
        send(x)
        y = await()
        send(y)
        z = await()
        send(z)

    f = flowlet(M)
    x = f.switch(1)

def test_schedule4():

    def M():
        x = await()
        y = await()
        z = await()
        send(x)
        send(y)
        send(z)

    f = flowlet(M)

    f.send(0)
    f.send(1)
    f.send(2)

    y0 = f.await()
    y1 = f.await()
    y2 = f.await()

    assert y0 == 0
    assert y1 == 1
    assert y2 == 2

def test_schedule5():

    def M():
        x = await()
        y = await()
        send(x+y)

    f = flowlet(M)
    f.send(1)
    f.send(2)

    y0 = f.await()
    assert y0 == 3

def test_switch1():

    def M():
        x = await()
        y = await()
        send(x+y)

    f = flowlet(M)
    f.switch(1)
    assert f.switch(2) == 3

def test_switch2():

    def M():
        send(1)
        send(2)
        send(3)

    f = flowlet(M)
    assert f.switch() == 1
    assert f.switch() == 2
    assert f.switch() == 3

def test_introspection():

    def M():
        send(1)
        send(2)
        send(3)

    f = flowlet(M)
    f.switch() == 1
    f.switch() == 2
    assert f.frame() != None
    assert f.opcode() != None

@skip
def test_binding():

    def M():
        send(1)
        send(2)
        send(3)

    def N():
        x = await()
        y = await()
        z = await()
        send(x)
        send(y)
        send(z)

    m = flowlet(M)
    n = flowlet(N)

    n.bind(m)

    assert n.up == m
    assert m.down == n

    assert n.switch() == 1
    assert n.switch() == 2
    assert n.switch() == 3

def test_finalize1():

    def M():
        raise RuntimeError("Boom")
        send(1)

    def N():
        close()

    m = flowlet(M)
    n = flowlet(N)

    n.bind(m)
    n.await()

def test_finalize2():

    def M():
        send(1)
        raise RuntimeError("Boom")
        send(2)

    def N():
        await()
        close()

    m = flowlet(M)
    n = flowlet(N)

    n.bind(m)
    n.switch()

def test_finalize3():

    def M():
        send(1)
        send(2)

    def N():
        await()
        close()
        await()
        await()

    m = flowlet(M)
    n = flowlet(N)

    n.bind(m)
    with assert_raises(BlockedUpstream):
        n.await()

def test_finalize4():

    def M():
        close()

    m = flowlet(M)
    with assert_raises(FlowletExit):
        m.switch()

def test_finalize5():

    def A():
        send(1)
        send(2)
        send(3)

    def B():
        send(await())
        send(0/0)
        send(await())

    def C():
        x = await()
        send(x)
        close()
        #await()

    a = flowlet(A)
    b = flowlet(B)
    c = flowlet(C)

    b.bind(a)
    c.bind(b)

    assert c.switch() == 1
    c.switch()

    assert not a.active
    assert not b.active
    assert not c.active

def test_finalize6():

    def A():
        assert False

    def B():
        close()
        await()

    a = flowlet(A)
    b = flowlet(B)

    b.bind(a)
    with assert_raises(BlockedUpstream):
        b.switch()

def test_finalize_chain():

    flags = [0,0,0]

    class resource(object):

        def __init__(self, flag, idx):
            self.flag = flag
            self.idx = idx

        def __enter__(self):
            pass

        def __exit__(self, exc_type, exc_value, traceback):
            # twiddle the flag
            self.flag[self.idx] = 1
            pass

    def A(flags):
        with resource(flags, 0):
            send('foo')

    def B(flags):
        with resource(flags, 1):
            x = await()
            send(x)

    def C(flags):
        with resource(flags, 2):
            await()
            close()

    a = flowlet(A, flags)
    b = flowlet(B, flags)
    c = flowlet(C, flags)

    b.bind(a)
    c.bind(b)
    c.switch()

    assert not c.active
    assert not b.active
    assert not a.active

    assert all(flags)

def test_finalize_order():

    stack = []

    class resource(object):

        def __init__(self, stack, idx):
            self.stack = stack
            self.idx = idx

        def __enter__(self):
            pass

        def __exit__(self, exc_type, exc_value, traceback):
            # twiddle the flag
            self.stack += [self.idx]
            pass

    def A(stack):
        with resource(stack, 0):
            send('foo')

    def B(stack):
        with resource(stack, 1):
            x = await()
            send(x)

    def C(stack):
        with resource(stack, 2):
            await()
            close()

    a = flowlet(A, stack)
    b = flowlet(B, stack)
    c = flowlet(C, stack)

    b.bind(a)
    c.bind(b)
    c.switch()

    assert not c.active
    assert not b.active
    assert not a.active

    # Close should unwind the stack from top to bottom
    assert stack == [0,1,2]

if __name__ == '__main__':
    import nose
    nose.runmodule(argv=[__file__,'-x','--pdb', '--pdb-failure'], exit=False)
