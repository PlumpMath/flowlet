#include <Python.h>
#include "greenlet.h"

typedef struct _flowlet {

    PyObject_HEAD
    PyGreenlet *gr;
    PyObject *value;
    PyObject *saturated;

    PyObject *run;
    PyObject *args;
    PyObject *kwargs;
    unsigned short iter;
    unsigned short pending;
    unsigned short suspended;
    unsigned short started;
    unsigned short finalized;

    unsigned short initial;
    unsigned short terminal;

    struct _flowlet *up;
    struct _flowlet *down;

} flowletobject;
