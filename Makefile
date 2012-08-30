# For local development, don't use this!

all:
	python setup.py build_ext --inplace --debug

clean:
	python setup.py clean

splint:
	splint --preproc flowlet/flow.c

valgrind:
	valgrind python tests/test_flow.py
	valgrind python tests/tests.py
