import asyncio
import ctypes
import os
from ctypes import c_int, c_char_p, c_void_p, Structure, POINTER, CFUNCTYPE
from typing import Optional, Callable

from uvloop import loop
from uvloop.loop import libuv_get_loop_t_ptr


uv = ctypes.CDLL(loop.__file__)


# Define libuv structures and functions
class uv_loop_t(Structure):
    _fields_ = [("data", c_void_p)]

class uv_fs_t(Structure):
    _fields_ = [
        # UV_REQ_FIELDS
        ("data", c_void_p),           # void* data
        ("type", c_int),              # uv_req_type type (typically 4 bytes)
        ("reserved", c_void_p * 6),   # void* reserved[6]
        # uv_fs_s specific fields
        ("fs_type", c_int),           # uv_fs_type
        ("loop", c_void_p),           # uv_loop_t* loop
        ("cb", c_void_p),             # uv_fs_cb cb
        ("result", c_int),            # ssize_t result
        ("ptr", c_void_p),            # void* ptr
        ("path", c_char_p),           # const char* path
        ("statbuf", c_void_p),        # uv_stat_t statbuf
    ]

# Define function prototypes
uv.uv_fs_open.argtypes = [POINTER(uv_loop_t), POINTER(uv_fs_t), c_char_p, c_int, c_int, c_void_p]
uv.uv_fs_open.restype = c_int

uv.uv_fs_req_cleanup.argtypes = [POINTER(uv_fs_t)]
uv.uv_fs_req_cleanup.restype = None

uv.uv_strerror.argtypes = [c_int]
uv.uv_strerror.restype = ctypes.c_char_p

# Define callback type
UV_FS_CB = CFUNCTYPE(None, POINTER(uv_fs_t))
    
# Set up the function signature for PyCapsule_GetPointer
ctypes.pythonapi.PyCapsule_GetPointer.restype = ctypes.c_void_p
ctypes.pythonapi.PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]

# Global registry to keep callbacks and requests alive
_callback_registry = []
_request_registry = []


def _get_uv_loop_ptr(loop: asyncio.AbstractEventLoop) -> POINTER:
    capsule = libuv_get_loop_t_ptr(loop)
    uv_loop_ptr = ctypes.pythonapi.PyCapsule_GetPointer(capsule, None)
    return ctypes.cast(uv_loop_ptr, POINTER(uv_loop_t))


def open(path: str, flags: int = os.O_RDONLY, mode: int = 0o644, *,
         loop: Optional[asyncio.AbstractEventLoop] = None) -> int:
    if loop is None:
        loop = asyncio.get_running_loop()
    
    uv_loop = _get_uv_loop_ptr(loop)
    
    req = uv_fs_t()
    _request_registry.append(req)
    
    fut = loop.create_future()
    
    def fs_callback(req_ptr):
        req = req_ptr.contents
        _request_registry.append(req)
        fut._req = req
        result = req.result
        
        if result < 0:
            error_str = uv.uv_strerror(result)
            error_msg = error_str.decode() if error_str else "Unknown error"
            fut.set_exception(OSError(result, error_msg))
        else:
            fut.set_result(result)
        
            uv.uv_fs_req_cleanup(req_ptr)
    
    cb = UV_FS_CB(fs_callback)
    _callback_registry.append(cb)
    
    result = uv.uv_fs_open(uv_loop, ctypes.byref(req), path.encode('utf-8'), flags, mode, cb)
    
    if result < 0:
        error_str = uv.uv_strerror(result)
        e = OSError(result, error_str.decode() if error_str else "Unknown error")
        fut.set_exception(e)
    
    def fut_done_callback(fut):
        _callback_registry.remove(cb)
        _request_registry.remove(fut._req)
        _request_registry.remove(req)

    fut.add_done_callback(fut_done_callback)

    return fut


__all__ = ['open']