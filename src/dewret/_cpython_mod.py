# Insert Python copyright
# This file: PSL 2.0

import builtins
import dis
from inspect import (
    ClosureVars,
    ismethod,
    isfunction,
    ismodule,
)
from typing import Callable, Any


def getclosurevars(func: Callable[..., Any]) -> ClosureVars:
    if ismethod(func):
        func = func.__func__

    if not isfunction(func):
        raise TypeError("{!r} is not a Python function".format(func))

    code = func.__code__
    # Nonlocal references are named in co_freevars and resolved
    # by looking them up in __closure__ by positional index
    if func.__closure__ is None:
        nonlocal_vars = {}
    else:
        nonlocal_vars = {
            var: cell.cell_contents
            for var, cell in zip(code.co_freevars, func.__closure__, strict=False)
        }

    # Global and builtin references are named in co_names and resolved
    # by looking them up in __globals__ or __builtins__
    global_ns = func.__globals__
    builtin_ns = global_ns.get("__builtins__", builtins.__dict__)
    if ismodule(builtin_ns):
        builtin_ns = builtin_ns.__dict__
    global_vars = {}
    builtin_vars = {}
    unbound_names = set()
    global_names = set()
    stackm1 = None
    for instruction in dis.get_instructions(code):
        opname = instruction.opname
        name = instruction.argval
        if opname == "LOAD_ATTR":
            unbound_names.add(f"{stackm1}.{name}")
            stackm1 = None
        elif opname == "LOAD_FAST":
            stackm1 = name
        elif opname == "LOAD_GLOBAL":
            global_names.add(name)
    for name in global_names:
        try:
            global_vars[name] = global_ns[name]
        except KeyError:
            try:
                builtin_vars[name] = builtin_ns[name]
            except KeyError:
                unbound_names.add(name)

    return ClosureVars(nonlocal_vars, global_vars, builtin_vars, unbound_names)
