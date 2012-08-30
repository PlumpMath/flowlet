#include "Python.h"
#include "opcode.h"
#include "structmember.h"

#include "flow.h"
#include "greenlet.h"

#include <stddef.h>
#include <stdlib.h>
#include <stdarg.h>
#include <frameobject.h>

#ifndef PY_SSIZE_T_MAX
typedef int Py_ssize_t;
#endif

#ifndef Py_TYPE
#  define Py_TYPE(ob)   (((PyObject *) (ob))->ob_type)
#endif

#define MODULE_NAME "flow"
#define FLOWLET_ACCESSOR "flowlet"
#define FLOWLET_NOOP Py_None
#define FLOWLET_PARAM PyTuple_New(0)

#define EmptyTuple(x) (PyTuple_CheckExact(x) && ((int)PyTuple_Size(x)) == 0)
#define Py_FlowletCheck(ob) (((PyObject *) (ob))->ob_type == &flowlet_type)
#define Py_FlowletFinalizing() PyErr_ExceptionMatches(PyExc_GreenletExit)

#ifdef DEBUG
#   define TRACE()    printf("%s:%s:%d\n",__FILE__,__FUNCTION__,__LINE__)
#else
#   define TRACE()    ((void) 0)
#endif

static PyTypeObject flowlet_type;
static PyObject *PyExc_FlowletExit;
static PyObject *PyExc_BlockedUpstream;

// ===========================
// Low level context switching
// ===========================

static PyObject *
f_switch(flowletobject *target, PyObject *args, PyObject *kwargs, unsigned short sending)
{
    PyObject *res;

#if DEBUG
    assert(Py_FlowletCheck(target));
#endif

    if (target->started == 1 && !PyGreenlet_ACTIVE(target->gr)) {
        PyErr_SetString(PyExc_RuntimeError, "Dead");
        Py_XDECREF(args);
        Py_XDECREF(kwargs);
        return NULL;
    }

    // Deferred argument passing from the constructor
    if (target->started == 0 && sending) {
        target->started = 1;
        PyGreenlet_Switch(target->gr, target->args, target->kwargs);
        res = PyGreenlet_Switch(target->gr, args, kwargs);
    } else if (target->started == 0 && !sending) {
        target->started = 1;
        res = PyGreenlet_Switch(target->gr, target->args, target->kwargs);
    } else {
        res = PyGreenlet_Switch(target->gr, args, kwargs);
    }

    return res;
}

// High level switching mechanics, with exception handling
static PyObject *
flowlet_switch(flowletobject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *state;

    int sending = (PySequence_Length(args) > 0) ||
                  (kwargs != NULL && PyDict_Size(kwargs) > 0);

#ifdef DEBUG
    if (PyErr_Occurred()) {
       return NULL;
    }
#endif

    state = f_switch(self, args, kwargs, sending);
    if (state == NULL) {
        // Shouldn't happen, but it can if you muck with the stack frames at runtime
        if (!PyErr_Occurred()) {
            PyErr_SetString(PyExc_RuntimeError, "Unknown context switch problem.");
        }
        return NULL;
    }
    return state;
}


// +---+     +---+
// | A | --> | B |
// +---+     +---+
//   ^         |
//   +---------+

static void
f_reflow(flowletobject *first, flowletobject *second)
{
    second->gr->parent = first->gr;
}

// +---+      +---+   +---+   +---+   +---+
// | A | -> [ | B | , | B | , | C | , | D | ]
// +---+      +---+   +---+   +---+   +-|-+
//   ^          +---<---+--<----+---<---+
//   |          |
//   +----------+

static void
f_rroll(flowletobject *a, PyObject *stack)
{
    PyObject *iterator = PyObject_GetIter(stack);
    flowletobject *i = a;
    flowletobject *j;

    while ((j = (flowletobject *)PyIter_Next(iterator))) {
        f_reflow(j, i);
        i = j;
        Py_DECREF(j);
    }
}

// +---+      +---+   +---+   +---+   +---+
// | A | -> [ | B | , | B | , | C | , | D | ]
// +---+      +-|-+   +---+   +---+   +---+
//   ^          +---<---+--<----+---<---+
//   |                                  |
//   +----------------------------------+

static void
f_lroll(flowletobject *a, PyObject *stack)
{
    PyObject *iterator = PyObject_GetIter(stack);
    flowletobject *i = a;
    flowletobject *j;

    while ((j = (flowletobject *)PyIter_Next(iterator))) {
        f_reflow(i, j);
        i = j;
        Py_DECREF(j);
    }
}

static PyObject *
flowlet_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    PyGreenlet *g;
    flowletobject *fl;

    PyObject *run = NULL;

    if (PyTuple_GET_SIZE(args) < 1) {
        PyErr_SetString(PyExc_TypeError, "type 'flowlet' takes at least one argument");
        return NULL;
    }

    fl = (flowletobject *)type->tp_alloc(type, 0);
    if (fl == NULL) {
        return NULL;
    }

    run = PyTuple_GET_ITEM(args, 0);

    fl->run = run;
    fl->args = PyTuple_GetSlice(args, 1, INT_MAX);
    fl->kwargs = kwargs;
    fl->terminal = 1;
    fl->initial = 1;

    g = (PyGreenlet *) PyType_GenericAlloc(&PyGreenlet_Type, 0);
#ifdef DEBUG
    if (g == NULL) {
        return NULL;
    }
#endif

    if (!PyCallable_Check(run)) {
        PyErr_SetString(PyExc_TypeError, "argument to 'run' is not callable");
        return NULL;
    }

    fl->gr = g;
    fl->saturated = Py_None;
    fl->pending = 0;

    PyGreenlet *parent = PyGreenlet_GetCurrent();
    PyGreenlet_SetParent(g, parent);

    g->dict = PyDict_New();
    Py_INCREF(g->dict);

    if (run != NULL) {
        Py_INCREF(run);
        g->run_info = run;
    }
    PyDict_SetItemString(g->dict, FLOWLET_ACCESSOR, (PyObject *)fl);

    return (PyObject *)fl;
}

// ===============
// Allocation / GC
// ===============

static void
flowlet_dealloc(flowletobject *self)
{
    Py_XDECREF(self->gr);
    Py_XDECREF(self->up);
    Py_XDECREF(self->down);
    Py_CLEAR(self->gr);
}

static int
flowlet_traverse(PyObject *self, visitproc visit, void *arg)
{
    flowletobject *fl = (flowletobject *)(self);
    Py_VISIT(fl->value);
    Py_VISIT(fl->saturated);
    Py_VISIT(fl->gr);
    Py_VISIT(fl->up);
    Py_VISIT(fl->down);
    return 0;
}

// ==========
// Properties
// ==========

static PyGreenlet *
flowlet_getgreenlet(flowletobject *self, void *c)
{
    PyGreenlet *result = self->gr;
    assert(result != NULL);
    Py_INCREF(result);
    return result;
}

static PyGreenlet *
flowlet_getup(flowletobject *self, void *c)
{
    PyGreenlet *result = (PyGreenlet *)self->up;
    assert(result != NULL);
    Py_INCREF(result);
    return result;
}

static PyGreenlet *
flowlet_getdown(flowletobject *self, void *c)
{
    PyGreenlet *result = (PyGreenlet *)self->down;
    assert(result != NULL);
    Py_INCREF(result);
    return result;
}

static PyObject *
flowlet_getsaturated(flowletobject *self, void *c)
{
    PyObject *sat = self->saturated;
    assert(sat != NULL);
    return sat;
}

static PyObject *
flowlet_getvalue(flowletobject *self, void *c)
{
    PyObject *result = self->value;
    if (self->value == NULL) {
        return Py_None;
    } else {
        Py_INCREF(result);
        return result;
    }
}

static PyObject *
flowlet_getargs(flowletobject *self, void *c)
{
    PyObject *result = self->args;
    assert(self->args != NULL);
    Py_INCREF(result);
    return result;
}

static PyObject *
flowlet_getkwargs(flowletobject *self, void *c)
{
    if (self->kwargs == NULL) {
        return PyDictProxy_New(PyDict_New());
    }
    return PyDictProxy_New(self->kwargs);
}

static PyObject *
flowlet_getactive(flowletobject *self, PyObject *args, PyObject **kwargs)
{
    if (PyGreenlet_ACTIVE(self) && PyGreenlet_STARTED(self)) {
        return Py_True;
    } else {
        return Py_False;
    }
}

static PyGetSetDef flowlet_getsets[] = {
    {"value"     , (getter)flowlet_getvalue     , NULL , NULL} ,
    {"args"      , (getter)flowlet_getargs      , NULL , NULL} ,
    {"kwargs"    , (getter)flowlet_getkwargs    , NULL , NULL} ,
    {"active"    , (getter)flowlet_getactive    , NULL , NULL} ,
#if DEBUG
    {"greenlet"  , (getter)flowlet_getgreenlet  , NULL , NULL} ,
    {"up"        , (getter)flowlet_getup        , NULL , NULL} ,
    {"down"      , (getter)flowlet_getdown      , NULL , NULL} ,
    {"saturated" , (getter)flowlet_getsaturated , NULL , NULL} ,
#endif
    {NULL}
};

PyDoc_STRVAR(flowlet_doc, "Flowlet");
PyDoc_STRVAR(switch_doc, "switch");

// f.send()
static PyObject *
flowlet_send(flowletobject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *result;

    if (PyErr_Occurred()) {
        return NULL;
    }

    if (self->suspended == 1) {
        PyErr_SetString(PyExc_ValueError, "Suspended");
        return NULL;
    }

    if (self->saturated == Py_True) {
        PyErr_SetString(PyExc_ValueError, "Already saturated.");
        return NULL;
    }

    Py_INCREF(args);
    Py_XINCREF(args);

    result = f_switch(self, args, NULL, 1);

    if (PyErr_Occurred()) {
        return NULL;
    }

    if (result != NULL && !EmptyTuple(result)) {
        self->pending = 1;
        self->value = result;
    }

    /*assert(result!=NULL);*/
    return Py_None;
}

// f.await()
static PyObject *
flowlet_await(flowletobject *self)
{
    PyObject *result;

    if (self->suspended == 1) {
        PyErr_SetString(PyExc_ValueError, "Suspended");
        return NULL;
    }

    if (self->started && self->saturated == Py_False) {
        PyErr_SetString(PyExc_ValueError, "Not saturated.");
        return NULL;
    }

    if (self->pending == 1) {

        if (PyErr_Occurred()) {
            return NULL;
        }

        assert(self->value != NULL);
        self->pending = 0;

        return self->value;
    } else {
        Py_CLEAR(self->value);
    }

    result = f_switch(self, NULL, NULL, 0);

    if (PyErr_Occurred()) {
        return NULL;
    }

    assert(result != NULL);
    /*assert (!EmptyTuple(result));*/

    return result;
}

static PyObject *
flowlet_final(flowletobject *self)
{
    PyObject *typ = PyExc_GreenletExit;
    Py_CLEAR(self->value);

    if (self->up != NULL) {
        f_reflow(self, self->up);
        flowlet_final(self->up);
    }

    if (self->gr == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "I'm dead already!");
        return NULL;
    }

    PyGreenlet_Throw(self->gr, typ, NULL, NULL);
    return Py_True;
}

static PyObject *
flowlet_resume(flowletobject *self)
{

    if (self->suspended == 0) {
        PyErr_SetString(PyExc_ValueError, "Not Suspended");
        return NULL;
    }

    self->suspended = 0;
    PyGreenlet_Switch(self->gr, NULL, NULL);
    return Py_None;
}

static PyObject *
flowlet_iternext(flowletobject *self)
{
    PyObject *result;
    result = flowlet_await(self);

    if (PyErr_Occurred()) {
        return NULL;
    }

    if (result == FLOWLET_NOOP) {
        PyErr_SetNone(PyExc_StopIteration);
        flowlet_final(self);
        return NULL;
    } else {
        Py_INCREF(result);
        return result;
    }
}

static PyObject *
flowlet_bind(flowletobject *self, PyObject *args, PyObject **kwargs)
{
    flowletobject *up;
    flowletobject *fup;

#if DEBUG
    assert(!self->started);
#endif

    if (!PyArg_UnpackTuple(args, "bind", 1, 1, &up)) {
        return NULL;
    }

    if (Py_TYPE(up) == &flowlet_type) {
        fup = up;
    } else {
        PyErr_SetString(PyExc_TypeError, "Not flowlet.");
        return NULL;
    }

    fup->initial = 1;
    self->initial = 0;

    fup->terminal = 0;
    self->terminal = 1;

    self->up = fup;
    fup->down  = self;

    // TODO: should this be a new flowlet?
    return Py_None;
}

static PyObject *
flowlet_frame(flowletobject *self)
{
    PyFrameObject *stackframe;
    PyObject *code = PyFunction_GetCode(self->run);
    PyObject *fcode;

    if (!PyGreenlet_ACTIVE(self->gr)) {
        PyErr_SetString(PyExc_RuntimeError, "Not running.");
        return NULL;
    }

    // Weirdness because PyFunction_GetCode doesn't actually get a
    // PyCodeObject, but whatever we just need it for pointer
    // arithmetic anyways
    stackframe = self->gr->top_frame;
    fcode = stackframe->f_code;
    while (stackframe != NULL) {
        if (!(fcode - code)) {
            return stackframe;
        }

        stackframe = stackframe->f_back;
        fcode = stackframe->f_code;
    }

    return Py_None;
}

// TODO: gcc keeps bitching about something in here
static PyObject *
flowlet_opcode(flowletobject *self)
{
    PyFrameObject *stackframe = flowlet_frame(self);
    PyObject *code = stackframe->f_code->co_code;
    Py_ssize_t instruction = stackframe->f_lasti;
    return PySequence_GetSlice(code, instruction, PySequence_Length(code));
}

static PyNumberMethods flowlet_operators = {
    0,                    /* nb _add */
    0,                    /* nb _subtract */
    0,                    /* nb _multiply */
    0,                    /* nb _divide */
    0,                    /* nb _remainder */
    0,                    /* nb _divmod */
    0,                    /* nb _power */
    0,                    /* nb _negative */
    0,                    /* nb _positive */
    0,                    /* nb _absolute */
    flowlet_getsaturated, /* nb _nonzero */
};

static PyMethodDef flowlet_methods[] = {
    {"send"   , (PyCFunction)flowlet_send   , METH_VARARGS                 , NULL}  ,
    {"await"  , (PyCFunction)flowlet_await  , METH_NOARGS                  , NULL}  ,
    {"final"  , (PyCFunction)flowlet_final  , METH_NOARGS                  , NULL}  ,
    {"resume" , (PyCFunction)flowlet_resume , METH_NOARGS                  , NULL}  ,
    {"switch" , (PyCFunction)flowlet_switch , METH_VARARGS | METH_KEYWORDS , NULL} ,
    {"bind"   , (PyCFunction)flowlet_bind   , METH_VARARGS | METH_KEYWORDS , NULL}  ,
    {"frame"  , (PyCFunction)flowlet_frame  , METH_NOARGS                  , NULL}  ,
    {"opcode" , (PyCFunction)flowlet_opcode , METH_NOARGS                  , NULL}  ,
    {NULL,      NULL}
};

static PyTypeObject flowlet_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "flow.flowlet",              /* tp_name */
    sizeof(flowletobject),       /* tp_basicsize */
    0,                           /* tp_itemsize */
    (destructor)flowlet_dealloc, /* tp_dealloc */
    0,                           /* tp_print */
    0,                           /* tp_getattr */
    0,                           /* tp_setattr */
    0,                           /* tp_compare */
    0,                           /* tp_repr */
    &flowlet_operators,          /* tp_as_number */
    0,                           /* tp_as_sequence */
    0,                           /* tp_as_mapping */
    0,                           /* tp_hash */
    0,                           /* tp_call */
    0,                           /* tp_str */
    PyObject_GenericGetAttr,     /* tp_getattro */
    0,                           /* tp_setattro */
    0,                           /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
    flowlet_doc,                 /* tp_doc */
    flowlet_traverse,            /* tp_traverse */
    0,                           /* tp_clear */
    0,                           /* tp_richcompare */
    0,                           /* tp_weaklistoffset */
    PyObject_SelfIter,           /* tp_iter */
    (iternextfunc)flowlet_iternext,/* tp_iternext */
    flowlet_methods,             /* tp_methods */
    0,                           /* tp_members */
    flowlet_getsets,             /* tp_getset */
    0,                           /* tp_base */
    0,                           /* tp_dict */
    0,                           /* tp_descr_get */
    0,                           /* tp_descr_set */
    0,                           /* tp_dictoffset */
    0,                           /* tp_init */
    0,                           /* tp_alloc */
    flowlet_new,                 /* tp_new */
    PyObject_GC_Del,             /* tp_free */
};

static flowletobject *
PyFlowlet_GetCurrent()
{
    flowletobject *fl;
    PyGreenlet *gr = PyGreenlet_GetCurrent();

    if (gr->dict == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "getcurrent() only usable within flowlet stack");
        return NULL;
    }

    fl = (flowletobject *)PyDict_GetItemString(gr->dict, FLOWLET_ACCESSOR);
    if (fl == NULL || PyErr_Occurred()) {
        return NULL;
    }
    return fl;
}


// ============================
// High level context swtiching
// ============================

static PyObject *
await(PyObject *self, PyObject *arg)
{
    PyObject *res;

    if (PyErr_Occurred()) {
        return NULL;
    }

    flowletobject *fl = PyFlowlet_GetCurrent();
    assert(fl != NULL);

    if (fl == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "await() only usable within flowlet stack");
        return NULL;
    }

    fl->saturated = Py_False;

    if (fl->initial && fl->terminal) {
        res = PyGreenlet_Switch(fl->gr->parent, FLOWLET_PARAM, NULL);
    } else if (fl->up != NULL) {
        res = f_switch(fl->up, FLOWLET_PARAM, NULL, 0);
    } else {
        PyErr_SetNone(PyExc_BlockedUpstream);
        return NULL;
    }

    if (PyErr_Occurred() && Py_FlowletFinalizing()) {
        PyErr_Clear();
    }

    return res;
}

static PyObject *
f_close(PyObject *self)
{
    flowletobject *fl = PyFlowlet_GetCurrent();

    if (fl == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "close() only usable within flowlet stack");
    }

    // Never connected to anything, so easy
    if (fl->initial && fl->terminal) {
        PyGreenlet_Throw(fl->gr, PyExc_FlowletExit, NULL, NULL);
        return NULL;
    }

    // Upstream never initialized, so easy
    if(fl->up->gr == NULL) {
        return Py_False;
    }

    // Upstream started but never ran, so easy
    if (!PyGreenlet_ACTIVE(fl->up->gr)) {
        Py_CLEAR(fl->up);
        return Py_False;
    }

    // We have a complicated stack to unwind upstream, so hard
    f_reflow(fl, fl->up);
    flowlet_final(fl->up);
    Py_CLEAR(fl->up);

    // Don't handle exceptions in the normal Python waybecause we're
    // they reflow back into our stack from upstream because of
    // the stack reflow.
    if (PyErr_Occurred() && Py_FlowletFinalizing()) {
        PyErr_Clear();
    }
    return Py_True;
}

static PyObject *
send(PyObject *self, PyObject *args, PyObject *kwargs)
{
    int i;
    PyFrameObject *f = PyEval_GetFrame();

    if (PyErr_Occurred()) {
        goto ctx_switch;
    }

    flowletobject *fl = PyFlowlet_GetCurrent();
    //assert(fl != NULL);

    fl->saturated = Py_True;
    Py_XINCREF(args);

    // Implictly forces any side-effects that are placed in the
    // arguments, i.e. socket.recv() or raw_input() calls would
    // be evaluated before performing context_switching
    if (fl->down == NULL) {
        PyGreenlet_Switch(fl->gr->parent, args, NULL);
    } else {
        f_switch(fl->down, args, NULL, 1);
    }

    if (PyErr_Occurred()) {
        goto ctx_switch;
    } else {
        return Py_None;
    }

ctx_switch:

    // Degenerate cases, trivial
    if (f == NULL || f->f_stacktop == NULL || f->f_iblock <= 0) {
        return NULL;
    }

    // Stack Unwinding Mechanics
    // ------------------------------------------
    // Examine the block stack of the current frame
    i = f->f_iblock;
    while (--i >= 0) {
        if (f->f_blockstack[i].b_type != SETUP_LOOP) {
            // Different exceptions depending on the block
            PyErr_SetNone(PyExc_FlowletExit);
            return NULL;
        } else {
            // Different exceptions depending on the block
            PyErr_SetNone(PyExc_GreenletExit);
            return NULL;
        }
    }
}

static PyObject *
suspend(PyObject *self, PyObject *args, PyObject *kwargs)
{
    flowletobject *fl = (flowletobject *)(PyFlowlet_GetCurrent());
    fl->suspended = 1;
    PyGreenlet_Switch(fl->gr->parent, NULL, NULL);
    return Py_None;
}

static PyObject *
get_flowlet(PyObject *self, PyObject *arg)
{
    flowletobject *fl = (flowletobject *)PyFlowlet_GetCurrent();

    if (fl == NULL) {
        return Py_None;
    } else {
        return (PyObject *)fl;
    }
}

// =====
// Utils
// =====

PyDoc_STRVAR(exhaust_doc, "Exhaust an interable, discarding all values.");

static PyObject *
pipes_exhaust(PyObject *self, PyObject *args)
{

    PyObject *it;
    PyObject *item;

    if (!PyArg_UnpackTuple(args, "exhaust", 1, 1, &it)) {
        return NULL;
    }

    it = PyObject_GetIter(it);
    if (it == NULL) {
        return NULL;
    }

    while (item = PyIter_Next(it)) {
        Py_DECREF(item);
    }
    Py_DECREF(it);

    if (PyErr_Occurred()) {
        return NULL;
    }
    return Py_True;
}

PyDoc_STRVAR(id_doc, "The identity function.");

static PyObject *
Id(PyObject *self, PyObject *arg)
{
    Py_INCREF(arg);
    return arg;
}


static PyMethodDef flow_methods[] = {
    {"await"      , await         , METH_NOARGS                  , NULL }        ,
    {"send"       , send          , METH_VARARGS | METH_KEYWORDS , NULL }        ,
    {"close"      , f_close       , METH_NOARGS                  , NULL }        ,
    {"suspend"    , suspend       , METH_VARARGS | METH_KEYWORDS , NULL }        ,
    {"getcurrent" , get_flowlet   , METH_NOARGS                  , NULL }        ,
    {"exhaust"    , pipes_exhaust , METH_VARARGS                 , exhaust_doc } ,
    {"Id"         , Id            , METH_O                       , id_doc }      ,
    {NULL      , NULL}
};

// ======
// Module
// ======

PyDoc_STRVAR(module_doc, "flowlets");

PyMODINIT_FUNC
initflow(void)
{
    PyGreenlet_Import();

    int i;
    char *name;
    PyObject *m;

    PyExc_FlowletExit = PyErr_NewException("flow.FlowletExit", PyExc_Exception, NULL);
    PyExc_BlockedUpstream = PyErr_NewException("flow.BlockedUpstream", PyExc_Exception, NULL);

    PyTypeObject *typelist[] = {
        &flowlet_type,
        NULL
    };

    // Inject Exceptions
    m = Py_InitModule3(MODULE_NAME, flow_methods, module_doc);

    Py_INCREF(PyExc_FlowletExit);
    PyModule_AddObject(m, "FlowletExit", PyExc_FlowletExit);
    PyModule_AddObject(m, "BlockedUpstream", PyExc_BlockedUpstream);

    if (m == NULL) {
        return;
    }

    for (i = 0 ; typelist[i] != NULL ; i++) {
        if (PyType_Ready(typelist[i]) < 0) {
            break;
        }

        name = strchr(typelist[i]->tp_name, '.');
        assert (name != NULL);

        Py_INCREF(typelist[i]);
        PyModule_AddObject(m, name + 1, (PyObject *)typelist[i]);
    }
}
