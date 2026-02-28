import asyncio
import ctypes
from ctypes import (
    CFUNCTYPE,
    POINTER,
    Structure,
    c_char,
    c_char_p,
    c_int,
    c_long,
    c_longlong,
    c_size_t,
    c_ssize_t,
    c_uint,
    c_uint64,
    c_void_p,
)
from typing import Any, Optional

import uvloop.loop  # type: ignore[import-not-found]
from uvloop.loop import libuv_get_loop_t_ptr  # type: ignore[import-not-found]


uv = ctypes.CDLL(uvloop.loop.__file__)


class uv_loop_t(Structure):
    _fields_ = [("data", c_void_p)]


class uv_fs_t(Structure):
    pass


class uv_buf_t(Structure):
    _fields_ = [
        ("base", POINTER(c_char)),
        ("len", c_size_t),
    ]


class uv_timespec_t(Structure):
    _fields_ = [
        ("tv_sec", c_long),
        ("tv_nsec", c_long),
    ]


class uv_stat_t(Structure):
    _fields_ = [
        ("st_dev", c_uint64),
        ("st_mode", c_uint64),
        ("st_nlink", c_uint64),
        ("st_uid", c_uint64),
        ("st_gid", c_uint64),
        ("st_rdev", c_uint64),
        ("st_ino", c_uint64),
        ("st_size", c_uint64),
        ("st_blksize", c_uint64),
        ("st_blocks", c_uint64),
        ("st_flags", c_uint64),
        ("st_gen", c_uint64),
        ("st_atim", uv_timespec_t),
        ("st_mtim", uv_timespec_t),
        ("st_ctim", uv_timespec_t),
        ("st_birthtim", uv_timespec_t),
    ]


class uv_fs_req_view_t(Structure):
    """Minimal prefix view to read public uv_fs_t result safely."""

    _fields_ = [
        ("data", c_void_p),  # void* data
        ("type", c_int),  # uv_req_type
        ("reserved", c_void_p * 6),  # void* reserved[6]
        ("fs_type", c_int),  # uv_fs_type
        ("loop", c_void_p),  # uv_loop_t* loop
        ("cb", c_void_p),  # uv_fs_cb cb
        ("result", c_ssize_t),  # ssize_t result
    ]


class uv_fs_req_stat_view_t(Structure):
    """uv_fs_t view that includes statbuf for uv_fs_fstat callbacks."""

    _fields_ = [
        ("data", c_void_p),  # void* data
        ("type", c_int),  # uv_req_type
        ("reserved", c_void_p * 6),  # void* reserved[6]
        ("fs_type", c_int),  # uv_fs_type
        ("loop", c_void_p),  # uv_loop_t* loop
        ("cb", c_void_p),  # uv_fs_cb cb
        ("result", c_ssize_t),  # ssize_t result
        ("ptr", c_void_p),  # void* ptr
        ("path", c_char_p),  # const char* path
        ("statbuf", uv_stat_t),  # uv_stat_t statbuf
    ]


uv.uv_fs_open.argtypes = [
    POINTER(uv_loop_t),
    POINTER(uv_fs_t),
    c_char_p,
    c_int,
    c_int,
    c_void_p,
]
uv.uv_fs_open.restype = c_int

uv.uv_fs_read.argtypes = [
    POINTER(uv_loop_t),
    POINTER(uv_fs_t),
    c_int,
    POINTER(uv_buf_t),
    c_uint,
    c_longlong,
    c_void_p,
]
uv.uv_fs_read.restype = c_int

uv.uv_fs_write.argtypes = [
    POINTER(uv_loop_t),
    POINTER(uv_fs_t),
    c_int,
    POINTER(uv_buf_t),
    c_uint,
    c_longlong,
    c_void_p,
]
uv.uv_fs_write.restype = c_int

uv.uv_fs_close.argtypes = [
    POINTER(uv_loop_t),
    POINTER(uv_fs_t),
    c_int,
    c_void_p,
]
uv.uv_fs_close.restype = c_int

uv.uv_fs_ftruncate.argtypes = [
    POINTER(uv_loop_t),
    POINTER(uv_fs_t),
    c_int,
    c_longlong,
    c_void_p,
]
uv.uv_fs_ftruncate.restype = c_int

uv.uv_fs_fsync.argtypes = [
    POINTER(uv_loop_t),
    POINTER(uv_fs_t),
    c_int,
    c_void_p,
]
uv.uv_fs_fsync.restype = c_int

uv.uv_fs_fstat.argtypes = [
    POINTER(uv_loop_t),
    POINTER(uv_fs_t),
    c_int,
    c_void_p,
]
uv.uv_fs_fstat.restype = c_int

uv.uv_fs_req_cleanup.argtypes = [POINTER(uv_fs_t)]
uv.uv_fs_req_cleanup.restype = None

uv.uv_req_size.argtypes = [c_int]
uv.uv_req_size.restype = c_size_t

uv.uv_strerror.argtypes = [c_int]
uv.uv_strerror.restype = ctypes.c_char_p

uv.uv_guess_handle.argtypes = [c_int]
uv.uv_guess_handle.restype = c_int

UV_FS_CB = CFUNCTYPE(None, POINTER(uv_fs_t))
UV_FS_REQ_TYPE = 6
UV_TTY_HANDLE_TYPE = 14

ctypes.pythonapi.PyCapsule_GetPointer.restype = ctypes.c_void_p
ctypes.pythonapi.PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]


class _FsRequestContext:
    __slots__ = ("req_buf", "refs", "cb")

    def __init__(self, req_buf: Any, refs: tuple[Any, ...]) -> None:
        self.req_buf = req_buf
        self.refs = refs
        self.cb: Optional[Any] = None


_pending_requests: dict[int, _FsRequestContext] = {}


_UVLOOP_REQUIRED_ERROR = (
    "uvfiles requires a uvloop event loop; call "
    "asyncio.set_event_loop_policy(uvloop.EventLoopPolicy()) before use"
)


def _req_addr_from_ptr(req_ptr: Any) -> int:
    addr = ctypes.cast(req_ptr, c_void_p).value
    if addr is None:
        raise RuntimeError("null request pointer")
    return int(addr)


def _alloc_fs_request(*refs: Any) -> tuple[Any, int]:
    req_size = uv.uv_req_size(UV_FS_REQ_TYPE)
    req_buf = ctypes.create_string_buffer(req_size)
    req_ptr = ctypes.cast(req_buf, POINTER(uv_fs_t))
    req_addr = ctypes.addressof(req_buf)
    _pending_requests[req_addr] = _FsRequestContext(req_buf, refs)
    return req_ptr, req_addr


def _set_request_callback(req_addr: int, cb: Any) -> None:
    context = _pending_requests.get(req_addr)
    if context is None:
        raise RuntimeError("request context missing before callback setup")
    context.cb = cb


def _cleanup_fs_request(req_ptr: Any, req_addr: Optional[int] = None) -> None:
    if req_addr is None:
        req_addr = _req_addr_from_ptr(req_ptr)

    context = _pending_requests.pop(req_addr, None)
    if context is None:
        return

    uv.uv_fs_req_cleanup(req_ptr)
    context.cb = None
    context.refs = ()
    context.req_buf = None


def _error_from_result(result: int) -> OSError:
    error_str = uv.uv_strerror(result)
    return OSError(result, error_str.decode() if error_str else "Unknown error")


def _get_uv_loop_ptr(event_loop: asyncio.AbstractEventLoop) -> Any:
    # Calling libuv_get_loop_t_ptr with a non-uvloop loop can abort the process
    # in some environments, so fail fast in Python before crossing the C boundary.
    if not type(event_loop).__module__.startswith("uvloop"):
        raise RuntimeError(_UVLOOP_REQUIRED_ERROR)

    try:
        capsule = libuv_get_loop_t_ptr(event_loop)
        uv_loop_ptr = ctypes.pythonapi.PyCapsule_GetPointer(capsule, None)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(_UVLOOP_REQUIRED_ERROR) from exc

    if not uv_loop_ptr:
        raise RuntimeError(_UVLOOP_REQUIRED_ERROR)

    return ctypes.cast(uv_loop_ptr, POINTER(uv_loop_t))
