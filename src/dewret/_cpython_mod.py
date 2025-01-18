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
    imports: dict[str, str] = {}
    for instruction in dis.get_instructions(code):
        opname = instruction.opname
        name = instruction.argval
        # TODO: this does not cover the possibility of a subsequent
        # variable shadowing an inline import
        if opname == "LOAD_ATTR" and stackm1 and (fullname := imports.get(stackm1)):
            unbound_names.add(f"{stackm1}.{name}:{fullname}.{name}")
            stackm1 = None
        elif opname == "IMPORT_NAME":
            # TODO: deal with STACK[-2]
            # Strictly, these only enter the namespace with a
            # subsequent STORE_FAST, present in normal circumstances.
            if stackm1 is not None:
                fullname = f"{stackm1}.{name}"
            else:
                fullname = name
            imports[name] = fullname
            stackm1 = name
        elif opname == "IMPORT_FROM":
            fullname = f"{stackm1}.{name}"
            unbound_names.add(f"{name}:{fullname}")
            imports[name] = fullname
            stackm1 = name
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
