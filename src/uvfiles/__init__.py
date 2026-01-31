import asyncio
import ctypes
import os
from ctypes import c_int, c_char_p, c_void_p, Structure, POINTER, CFUNCTYPE
from typing import Optional, List, Iterator, Any

from uvloop import loop
from uvloop.loop import libuv_get_loop_t_ptr


uv = ctypes.CDLL(loop.__file__)


class AsyncFile:
    """Async file object compatible with Python's built-in file object interface."""

    def __init__(self, fd: int, name: str, mode: str = "r") -> None:
        self._fd = fd
        self._name = name
        self._mode = mode
        self._closed = False

    @property
    def name(self) -> str:
        """The file name."""
        return self._name

    @property
    def mode(self) -> str:
        """The file mode."""
        return self._mode

    @property
    def closed(self) -> bool:
        """True if the file is closed."""
        return self._closed

    def fileno(self) -> int:
        """Return the file descriptor."""
        if self._closed:
            raise ValueError("I/O operation on closed file")
        return self._fd

    def readable(self) -> bool:
        """Return True if the file is readable."""
        return "r" in self._mode or "+" in self._mode

    def writable(self) -> bool:
        """Return True if the file is writable."""
        return "w" in self._mode or "a" in self._mode or "+" in self._mode

    def seekable(self) -> bool:
        """Return True if the file is seekable."""
        raise NotImplementedError("seekable() is not implemented yet")

    def read(self, size: int = -1) -> bytes:
        """Read and return up to size bytes."""
        raise NotImplementedError("read() is not implemented yet")

    def readline(self, size: int = -1) -> bytes:
        """Read and return one line from the file."""
        raise NotImplementedError("readline() is not implemented yet")

    def readlines(self, hint: int = -1) -> List[bytes]:
        """Read and return a list of lines from the file."""
        raise NotImplementedError("readlines() is not implemented yet")

    def write(self, data: bytes) -> int:
        """Write data to the file."""
        raise NotImplementedError("write() is not implemented yet")

    def writelines(self, lines: List[bytes]) -> None:
        """Write a list of lines to the file."""
        raise NotImplementedError("writelines() is not implemented yet")

    def seek(self, offset: int, whence: int = 0) -> int:
        """Change the stream position."""
        raise NotImplementedError("seek() is not implemented yet")

    def tell(self) -> int:
        """Return the current stream position."""
        raise NotImplementedError("tell() is not implemented yet")

    def truncate(self, size: Optional[int] = None) -> int:
        """Truncate the file to at most size bytes."""
        raise NotImplementedError("truncate() is not implemented yet")

    def flush(self) -> None:
        """Flush the write buffers."""
        raise NotImplementedError("flush() is not implemented yet")

    def close(self) -> None:
        """Close the file."""
        raise NotImplementedError("close() is not implemented yet")

    def __enter__(self) -> "AsyncFile":
        """Enter the runtime context."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the runtime context and close the file."""
        self.close()

    def __iter__(self) -> Iterator[bytes]:
        """Iterate over lines in the file."""
        raise NotImplementedError("__iter__() is not implemented yet")

    def __next__(self) -> bytes:
        """Return the next line from the file."""
        raise NotImplementedError("__next__() is not implemented yet")

    def __repr__(self) -> str:
        return f"<AsyncFile name={self._name!r} mode={self._mode!r} closed={self._closed}>"


# Define libuv structures and functions
class uv_loop_t(Structure):
    _fields_ = [("data", c_void_p)]


class uv_fs_t(Structure):
    _fields_ = [
        # UV_REQ_FIELDS
        ("data", c_void_p),  # void* data
        ("type", c_int),  # uv_req_type type (typically 4 bytes)
        ("reserved", c_void_p * 6),  # void* reserved[6]
        # uv_fs_s specific fields
        ("fs_type", c_int),  # uv_fs_type
        ("loop", c_void_p),  # uv_loop_t* loop
        ("cb", c_void_p),  # uv_fs_cb cb
        ("result", c_int),  # ssize_t result
        ("ptr", c_void_p),  # void* ptr
        ("path", c_char_p),  # const char* path
        ("statbuf", c_void_p),  # uv_stat_t statbuf
    ]


# Define function prototypes
uv.uv_fs_open.argtypes = [
    POINTER(uv_loop_t),
    POINTER(uv_fs_t),
    c_char_p,
    c_int,
    c_int,
    c_void_p,
]
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


def _flags_to_mode(flags: int) -> str:
    """Convert os.O_* flags to file mode string."""
    if flags & os.O_RDWR:
        if flags & os.O_APPEND:
            return "a+"
        elif flags & os.O_CREAT:
            return "w+" if flags & os.O_TRUNC else "r+"
        return "r+"
    elif flags & os.O_WRONLY:
        if flags & os.O_APPEND:
            return "a"
        return "w"
    else:  # O_RDONLY
        return "r"


def open(
    path: str,
    flags: int = os.O_RDONLY,
    mode: int = 0o644,
    *,
    loop: Optional[asyncio.AbstractEventLoop] = None,
) -> asyncio.Future[AsyncFile]:
    if loop is None:
        loop = asyncio.get_running_loop()

    uv_loop = _get_uv_loop_ptr(loop)

    req = uv_fs_t()
    _request_registry.append(req)

    fut = loop.create_future()
    file_mode = _flags_to_mode(flags)

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
            fut.set_result(AsyncFile(result, path, file_mode))

            uv.uv_fs_req_cleanup(req_ptr)

    cb = UV_FS_CB(fs_callback)
    _callback_registry.append(cb)

    result = uv.uv_fs_open(
        uv_loop, ctypes.byref(req), path.encode("utf-8"), flags, mode, cb
    )

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


__all__ = ["open", "AsyncFile"]
