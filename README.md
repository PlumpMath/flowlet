Flowlets are a small concurrency structure for building stream pipelines
through coroutines. They can be thought of as "Greenlets with additional
structure" or as a way of making greenlet control flow look more like
Haskell functor composition.

What is it?
===========

* Tiny C core that provides a new ``flowlet`` type to interpreter.
* Prelude of standard combinators and function for working with streamable
  Python types.

The full library can be thought of as a simple DSL with functional
syntax for directing data traversal and flow.

At the moment only CPython is supported.

Usage
-----

The primary data structure is a ``Pipe``. A pipe is a a coroutine
that can be composed with other coroutines to produce a stream of data.
Streams are lazy by default and consumer-driven. Data is only produced
as a result of the entire pipeline being called with ``runPipeline()``.

<p align="center" style="padding: 20px">
    <img src="http://raw.github.com/sdiehl/flowlet/master/img/pipe.png"/>
</p>

There is one composition operator which is defined for all
subclasses of Pipe.

```python
a >> b
```

<p align="center" style="padding: 20px">
    <img src="http://raw.github.com/sdiehl/flowlet/master/img/comp.png "/>
</p>

And an identity Pipe which consists of a pair of mappings which map
Pipes to themselves and their elements in the pipe to themselves.

```python
idP :: (a ~> b) -> (a ~> b)

Id :: x -> x
Id = lambda x: x
```

The composition of pipes is given by four combinators and pattern
matching:

```python
(a >> b) . (c >> d) = A1
(a >> b) . c        = A2
a . (b >> c)        = A3
a . b               = A4
```

With:

```haskell
A1 = \x,y -> g x $ f Id y
A2 = \x,y -> x $ f B y
A3 = \x,y -> f x (A y)
A4 = \x,y -> x $ B $ A y
```

```python

def example1():
    a = forever(raw_input)
    b = take(3)

    line = runPipeline(a >> b)
    print 'Output', line

>>> example1()
foo
bar
Output  ['foo', 'bar']
```

A Haskell analogue might look something like this:

```haskell
newtype (Compose g f) x = C { unCompose :: g (f x) }
instance (Functor f, Functor g) => Functor (Compose g f) where
	fmap f (C xs) = C $ fmap (fmap f) xs
```

One can show (see PROOFS.md ) that all orders of composition of (A1,
A2, A3, A4 ) are necessarily associative. Ergo, the following are all
equivalent:

```python
(a >> b) >> c   =   a >> (b >> c)    =   a >> b >> c
```

<p align="center" style="padding: 20px">
    <img src="http://raw.github.com/sdiehl/flowlet/master/img/assoc.png"/>
</p>

And the identities have the usual properties.

```python
a >> idP   =   idP >> a   =   a
```

<p align="center" style="padding: 20px">
    <img src="http://raw.github.com/sdiehl/flowlet/master/img/id.png"/>
</p>

Pipes can be composed to form arbitrarily complex logic and can
also be mixed in with iterable Python primitives, generators, and
itertools constructs.

```python
def example2():

    def my_generator():
        yield 1
        yield 2
        yield 3

    a = my_generator()
    b = pipe(lambda x: x**2)
    c = take(3)

    line = runPipeline(a >> b >> c)
    print line
```

This is also possible using Vanilla CPython of course. The more
interesting Pipe is called a ``flowlet``. A flowlet has multiple
entry and exit points and thus composition of them can be thought of
independent stacks running "simultaenously" and calling back into each
other.

There are two modes of operation ``await()`` and ``send()``. The
``await`` operation waits on a value upstream while ``send`` sends a
value downstream.

```python
def example3():

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

    A = layer1()
    B = layer2()
    C = layer3()

    line = A >> B >> C
    result = runPipeline(line)
    print result
    # [3, 9]
```

The graph of the flow through the stacks looks something like
this:

```
    Layer1          Layer2             Layer3          Main

    send(1) <-- x = await() <------------ a = await()
       |        |     |                   ^     |
       +---->---+     |                   |     v
                      v                   |   send(a) --> 3
    send(2) <-- y = await()               |     |
       |        |     |                   |     |
       +---->---+     |                   |     |
                      v                   |     |
                    send(x+y)             |     |
                          |               |     |
                          + ---------------     |
                                                v
    send(3) <-- z = await()  <----------- b = await()
       |        |     |                   ^     |
       +---->---+     v                   |     v
                    send(x+y+z)           |   send(b) --> 9
                          |               |
                          + ---------------
```

Finalization
------------

A flowlet most often reads from a finite stream of data, and when data
is no longer needed downstream we expect that all upstream pipes and
their associated resource handlers should exit immediately.

In Python especially most often a flowlet is doing some impure
operation ( i.e. reading a file, reading from IPC, poling a ZMQ socket,
etc). When a downstream source no longer requires data we want the
pipeline to handle closing that resource automatically.

To that end there is a ``close()`` function. The flowlets will
then terminate all upstream values, taking care to unwind the
stacks in such a way that resource handlers on ``with`` exit in a
well-defined order and synchronized with the exception raising.

```python
def A():
    while True:
        send(1)

def B():
    await()
    close()

a = A()
b = B()

pipe = a >> b
runPipeline(pipe)
```

Prelude
-------

Python of course doesn't have a type system ( or it's a trivial one with
a single type and all partial functions ), so when we say the Pipe of
``( a ~> b )`` we mean something amorphous like "from the space of inputs
called a" to the "space of outputs called b".  Ergo these signatures are
mostly gratitious and just byproduct of me trying to think through the
structure of the Prelude.

The core combinators.

```haskell
(>>) :: (a ~> b) -> ( c ~> d ) -> ( a ~> d )
(|)  :: (a ~> b) -> [b]
id   :: (a ~> a)
```

The lifting operations which lift Python functions into the
pipeline.

```haskell
pipe  :: (a -> b) -> a ~> b
pipe_ :: (a -> b) -> a ~> ()
```

The print operations, which print the contents of the stream
either passing it through or discarding it.

```haskell
printer  :: (a -> b) -> a ~> b
printer_ :: (a -> b) -> a ~> ()
```

The control flow operations.

```haskell
forever :: (a -> b) -> a ~> b
take    :: Integer -> (a ~> b)
collect :: Integer -> (a ~> [a])
consume :: (a ~> ())
repeat  :: (a -> b) -> a ~> b
forM    :: (a -> b) -> Integer -> a ~> b
barrier :: (a -> Bool) ~> (a ~> b)
```

The multiple stream operations which act of tuple element streams.

```haskell
fst     :: (a -> b) -> (a,x) ~> (b,x)
snd     :: (a -> b) -> (x,a) ~> (x,b)
split   :: a ~> (a,a)
unsplit :: (a -> b -> c) -> (a,b) ~> c
dimap   :: (a -> b) -> (a -> c) -> (a ~> (b,c))
parmap  :: (a -> b) -> (c -> d) -> ((a,b) ~> (c,d))
```

<p align="center" style="padding: 20px">
    <img src="http://raw.github.com/sdiehl/flowlet/master/img/split.png"/>
    <img src="http://raw.github.com/sdiehl/flowlet/master/img/unsplit.png"/>
</p>

<p align="center" style="padding: 20px">
    <img src="http://raw.github.com/sdiehl/flowlet/master/img/dimap.png"/>
    <img src="http://raw.github.com/sdiehl/flowlet/master/img/parmap.png"/>
</p>

Higher order "injection" operations. Which take single feeds and
inject a second stream.

```haskell
first  :: (() ~> a) -> (x ~> (a, x))
second :: (() ~> a) -> (x ~> (x, a))
```

<p align="center" style="padding: 20px">
    <img src="http://raw.github.com/sdiehl/flowlet/master/img/first.png"/>
    <img src="http://raw.github.com/sdiehl/flowlet/master/img/second.png"/>
</p>

The IO operations which lift outside data sources into the
pipeline.

```haskell
filepipe  :: String -> (a ~> b)
queuepipe :: Queue -> (a ~> b)
ipcpipe   :: IPC -> (a ~> b)
sockpipe  :: Socket -> (a ~> b)
```

And the optional Cython Pipe which compiles the given
logic into C and then inlines it in the Pipe.

```haskell
cythonpipe :: (a -> b) -> (a ~> b)
numexprpipe :: String -> (a ~> b)
```

And parllel operators and functions.

```haskell
par        :: [(a ~> b)] -> ([a] ~> [b])
gather     :: Integer -> [a] ~> b
allgather  :: Integer -> [a] ~> b
scatter    :: Integer -> a ~> [b]
roundrobin :: Integer -> a ~> [b]
```

Parallelism
===========

Because of the associativity of pipeline composition and functional
construction most pipelines have the property that it and all subsets
are trivially parallelizable and even more importantly is that
*the semantics of parallel flow remain the same as single threaded flow*.

To construct this we introduce the higher order pipe ``par`` which maps
flow into separate Processes.

<p align="center" style="padding: 20px">
    <img src="http://raw.github.com/sdiehl/flowlet/master/img/par1.png"/>
</p>

```python
f = a >> b >> c
g = x >> y >> z

p1 = A >> par(f,g) >> B    # parallel pipelines in different processes

runPipeline(p1)
```


``runPipeline(p1)`` can itself be expressed in other runPipeline
calls to other runPipeline's in different processes:


```python
 par (a ~> b) (a ~> d)
   --> runPipeline ( a ~> b )
   --> runPipeline ( a ~> d )
```

To move data in and out of  pipeline there are functions ``scatter``, ``gather``, 
and ``gatherall`` which have similar functionality to their MPI equivelants.

<p align="center" style="padding: 20px">
    <img src="http://raw.github.com/sdiehl/flowlet/master/img/scatter.png"/>
    <img src="http://raw.github.com/sdiehl/flowlet/master/img/gather.png"/>
    <img src="http://raw.github.com/sdiehl/flowlet/master/img/allgather.png"/>
</p>

This lets us build composite pipelines where portions of the pipeline run
on different computation contexts to other parts and the scheduling
arises out of the composition semantics.

Numeric Pipelines
=================

Numeric operators can also be used in a pipeline. For example one
can lift any numpy function into the pipeline and work with numpy
arrays passed as values through the pipeline.

```text
Some imaginary numeric pipeline
[ 1 1 1 ]   [ 1 2 0 ]      [ 1 0 0 ]   [ 1 0 0 ]
[ 0 1 1 ] , [ 0 0 0 ] -->  [ 1 1 0 ] , [ 2 0 2 ]
[ 0 0 1 ]   [ 0 2 1 ]      [ 1 1 1 ]   [ 0 0 1 ]
```

```python
from numpy import transpose

T = pipe(transpose)
tasks = par(T, N=3)

runPipeline(tasks)
```

Examples
========

See ``/examples.py`` or the large unit test suite:

To run the test suite:

```bash
$ nosetests
.............................................................................
.........................................................................
-----------------------------------------------------------------------------
150 tests run in 0.1 seconds (150 tests passed)
```

Inspirations
============

The library was very heavily inspired by the Haskell ``pipes`` library
by Gabriel Gonzalez. Though a Python interpretation will only ever be a
weak imitation of the ideas found in pipes due to Python's shortcomings.

The scheduler was inspired by the article ``Coroutine
Pipelines`` by Mario Blažević in the Monad Reader Issue #19 and the
[corresponding library](http://hackage.haskell.org/package/monad-coroutine).

Caveats
=======

This is uber-experimental code, don't use it for anything! It is
also *very* not threadsafe at the moment.

License
=======

The code and documentation are released under MIT License. You are
welcome to use it for any purpose academically or commercially, but I
kindly ask you (instead of legally obligating) that any derivative works
and patches find their way back into the community.
