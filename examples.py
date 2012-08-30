from flowlet.pipeline import *
from flowlet.prelude import *

def example1():

    a = forever(raw_input)
    b = take(3)

    line = runPipeline(a >> b)
    print line

def example2():

    a = forever(raw_input)
    b = pipe(lambda x: "You typed: %r" % x)
    c = take(2)

    line = runPipeline(a >> b >> c)
    print line

def example3():

    def my_generator():
        yield 1
        yield 2
        yield 3

    a = my_generator()
    b = pipe(lambda x: x**2)
    c = take(3)

    line = runPipeline(a >> b >> c)
    print line

def example3():

    @lazy
    def addone(input):
        for i in input:
            yield i+1

    line = count(0) >> addone() >> take(5)
    result = runPipeline(line)
    print result


def example4():


    @flowlet
    def layer1():
        send(1)
        send(2)
        send(3)

    @flowlet
    def layer2():
        x = await()
        y = await()
        send(x+y)

        z = await()
        send(x+y+z)

    @flowlet
    def layer3():
        a = await()
        send(a)

        b = await()
        send(a+b)

    line = layer1() >> layer2() >> layer3()
    result = runPipeline(line)
    print result

example1()
example2()
example3()
example4()
