import asyncio
import ctypes
import os
from ctypes import (
    c_char,
    c_int,
    c_char_p,
    c_longlong,
    c_size_t,
    c_ssize_t,
    c_uint,
    c_void_p,
    Structure,
    POINTER,
    CFUNCTYPE,
)
from typing import Optional, List, Iterator, Any

from uvloop import loop
from uvloop.loop import libuv_get_loop_t_ptr


uv = ctypes.CDLL(loop.__file__)


class AsyncFile:
    """Async file object compatible with Python's built-in file object interface."""

    def __init__(
        self,
        fd: int,
        name: str,
        loop: asyncio.AbstractEventLoop,
        mode: str = "r",
    ) -> None:
        self._fd = fd
        self._name = name
        self._loop = loop
        self._uv_loop = _get_uv_loop_ptr(loop)
        self._mode = mode
        self._pos = 0
        self._closed = False
        self._read_buffer = bytearray()

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
        self._ensure_open()
        return True

    async def read(self, size: int = -1) -> bytes:
        """Read and return up to size bytes."""
        self._ensure_open()
        self._ensure_loop()

        if size == 0:
            return b""

        if size > 0:
            chunks = []
            remaining = size

            if self._read_buffer:
                buffered = self._consume_from_read_buffer(remaining)
                if buffered:
                    chunks.append(buffered)
                    remaining -= len(buffered)

            if remaining > 0:
                data = await self._read_once(remaining, self._pos)
                self._pos += len(data)
                chunks.append(data)

            return b"".join(chunks)

        chunks = []
        chunk_size = 64 * 1024

        if self._read_buffer:
            chunks.append(self._consume_from_read_buffer(len(self._read_buffer)))

        while True:
            data = await self._read_once(chunk_size, self._pos)
            if not data:
                break
            chunks.append(data)
            self._pos += len(data)

        return b"".join(chunks)

    async def readline(self, size: int = -1) -> bytes:
        """Read and return one line from the file."""
        self._ensure_open()
        self._ensure_loop()

        if size == 0:
            return b""

        limit = None if size < 0 else size
        chunks = []
        total = 0

        while True:
            if self._read_buffer:
                remaining = None if limit is None else limit - total
                if remaining == 0:
                    return b"".join(chunks)

                scan_end = (
                    len(self._read_buffer)
                    if remaining is None
                    else min(len(self._read_buffer), remaining)
                )
                newline_idx = self._read_buffer.find(b"\n", 0, scan_end)

                if newline_idx != -1:
                    chunks.append(self._consume_from_read_buffer(newline_idx + 1))
                    return b"".join(chunks)

                if remaining is not None and scan_end == remaining:
                    chunks.append(self._consume_from_read_buffer(scan_end))
                    return b"".join(chunks)

                take = len(self._read_buffer)
                chunks.append(self._consume_from_read_buffer(take))
                total += take

            read_size = 8 * 1024
            if limit is not None:
                remaining = limit - total
                if remaining <= 0:
                    return b"".join(chunks)
                read_size = min(read_size, remaining)

            data = await self._read_once(read_size, self._pos)
            if not data:
                return b"".join(chunks)
            self._read_buffer.extend(data)

    async def readlines(self, hint: int = -1) -> List[bytes]:
        """Read and return a list of lines from the file."""
        self._ensure_open()
        self._ensure_loop()

        lines = []
        total = 0

        while True:
            line = await self.readline()
            if line == b"":
                break

            lines.append(line)
            total += len(line)

            if hint > 0 and total >= hint:
                break

        return lines

    async def write(self, data: bytes) -> int:
        """Write data to the file."""
        self._ensure_open()
        self._ensure_loop()

        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("a bytes-like object is required, not str")

        payload = bytes(data)
        if not payload:
            return 0

        written = await self._write_once(payload, self._pos)
        self._pos += written
        if written:
            self._read_buffer.clear()
        return written

    async def writelines(self, lines: List[bytes]) -> None:
        """Write a list of lines to the file."""
        self._ensure_open()
        self._ensure_loop()

        for line in lines:
            await self.write(line)

    def seek(self, offset: int, whence: int = 0) -> int:
        """Change the stream position."""
        self._ensure_open()

        if whence == os.SEEK_SET:
            new_pos = offset
        elif whence == os.SEEK_CUR:
            new_pos = self._pos + offset
        elif whence == os.SEEK_END:
            new_pos = os.fstat(self._fd).st_size + offset
        else:
            raise ValueError("invalid whence")

        if new_pos < 0:
            raise ValueError("negative seek position")

        self._pos = new_pos
        self._read_buffer.clear()
        return self._pos

    def tell(self) -> int:
        """Return the current stream position."""
        self._ensure_open()
        return self._pos

    async def truncate(self, size: Optional[int] = None) -> int:
        """Truncate the file to at most size bytes."""
        self._ensure_open()
        self._ensure_loop()

        target_size = self._pos if size is None else size
        if target_size < 0:
            raise ValueError("negative size value")

        await self._truncate_once(target_size)
        self._read_buffer.clear()

        if self._pos > target_size:
            self._pos = target_size

        return target_size

    async def flush(self) -> None:
        """Flush the write buffers."""
        self._ensure_open()
        self._ensure_loop()
        await self._fsync_once()

    async def close(self) -> None:
        """Close the file."""
        if self._closed:
            return

        self._ensure_loop()
        await self._close_once()
        self._closed = True

    def __enter__(self) -> "AsyncFile":
        """Enter the runtime context."""
        raise TypeError("AsyncFile only supports async context manager, use 'async with'")

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the runtime context and close the file."""
        raise TypeError("AsyncFile only supports async context manager, use 'async with'")

    async def __aenter__(self) -> "AsyncFile":
        self._ensure_open()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    def __aiter__(self) -> "AsyncFile":
        self._ensure_open()
        return self

    async def __anext__(self) -> bytes:
        line = await self.readline()
        if line == b"":
            raise StopAsyncIteration
        return line

    def __iter__(self) -> Iterator[bytes]:
        """Iterate over lines in the file."""
        raise TypeError("AsyncFile is asynchronously iterable, use 'async for'")

    def __next__(self) -> bytes:
        """Return the next line from the file."""
        raise TypeError("AsyncFile is asynchronously iterable, use 'async for'")

    def __repr__(self) -> str:
        return f"<AsyncFile name={self._name!r} mode={self._mode!r} closed={self._closed}>"

    def _ensure_open(self) -> None:
        if self._closed:
            raise ValueError("I/O operation on closed file")

    def _ensure_loop(self) -> None:
        running_loop = asyncio.get_running_loop()
        if running_loop is not self._loop:
            raise RuntimeError("AsyncFile is bound to a different event loop")

    def _consume_from_read_buffer(self, size: int) -> bytes:
        available = min(size, len(self._read_buffer))
        if available <= 0:
            return b""

        data = bytes(self._read_buffer[:available])
        del self._read_buffer[:available]
        self._pos += available
        return data

    async def _read_once(self, size: int, offset: int) -> bytes:
        fut = self._loop.create_future()

        read_buffer = ctypes.create_string_buffer(size)
        uv_bufs = (uv_buf_t * 1)(
            uv_buf_t(ctypes.cast(read_buffer, POINTER(c_char)), size)
        )

        req_ptr, req_addr = _alloc_fs_request(read_buffer, uv_bufs)

        def fs_callback(req_ptr):
            req_view = ctypes.cast(req_ptr, POINTER(uv_fs_req_view_t)).contents
            result = req_view.result

            try:
                if result < 0:
                    if not fut.done():
                        fut.set_exception(_error_from_result(result))
                else:
                    data = read_buffer.raw[: int(result)]
                    if not fut.done():
                        fut.set_result(data)
            finally:
                _cleanup_fs_request(req_ptr)

        cb = UV_FS_CB(fs_callback)
        _set_request_callback(req_addr, cb)

        result = uv.uv_fs_read(
            self._uv_loop, req_ptr, self._fd, uv_bufs, 1, offset, cb
        )
        if result < 0:
            _cleanup_fs_request(req_ptr, req_addr)
            raise _error_from_result(result)

        return await fut

    async def _write_once(self, payload: bytes, offset: int) -> int:
        fut = self._loop.create_future()

        write_buffer = ctypes.create_string_buffer(payload, len(payload))
        uv_bufs = (uv_buf_t * 1)(
            uv_buf_t(ctypes.cast(write_buffer, POINTER(c_char)), len(payload))
        )

        req_ptr, req_addr = _alloc_fs_request(write_buffer, uv_bufs)

        def fs_callback(req_ptr):
            req_view = ctypes.cast(req_ptr, POINTER(uv_fs_req_view_t)).contents
            result = req_view.result

            try:
                if result < 0:
                    if not fut.done():
                        fut.set_exception(_error_from_result(result))
                else:
                    if not fut.done():
                        fut.set_result(int(result))
            finally:
                _cleanup_fs_request(req_ptr)

        cb = UV_FS_CB(fs_callback)
        _set_request_callback(req_addr, cb)

        result = uv.uv_fs_write(
            self._uv_loop, req_ptr, self._fd, uv_bufs, 1, offset, cb
        )
        if result < 0:
            _cleanup_fs_request(req_ptr, req_addr)
            raise _error_from_result(result)

        return await fut

    async def _close_once(self) -> None:
        fut = self._loop.create_future()
        req_ptr, req_addr = _alloc_fs_request()

        def fs_callback(req_ptr):
            req_view = ctypes.cast(req_ptr, POINTER(uv_fs_req_view_t)).contents
            result = req_view.result

            try:
                if result < 0:
                    if not fut.done():
                        fut.set_exception(_error_from_result(result))
                else:
                    if not fut.done():
                        fut.set_result(None)
            finally:
                _cleanup_fs_request(req_ptr)

        cb = UV_FS_CB(fs_callback)
        _set_request_callback(req_addr, cb)

        result = uv.uv_fs_close(self._uv_loop, req_ptr, self._fd, cb)
        if result < 0:
            _cleanup_fs_request(req_ptr, req_addr)
            raise _error_from_result(result)

        await fut

    async def _truncate_once(self, size: int) -> None:
        fut = self._loop.create_future()
        req_ptr, req_addr = _alloc_fs_request()

        def fs_callback(req_ptr):
            req_view = ctypes.cast(req_ptr, POINTER(uv_fs_req_view_t)).contents
            result = req_view.result

            try:
                if result < 0:
                    if not fut.done():
                        fut.set_exception(_error_from_result(result))
                else:
                    if not fut.done():
                        fut.set_result(None)
            finally:
                _cleanup_fs_request(req_ptr)

        cb = UV_FS_CB(fs_callback)
        _set_request_callback(req_addr, cb)

        result = uv.uv_fs_ftruncate(self._uv_loop, req_ptr, self._fd, size, cb)
        if result < 0:
            _cleanup_fs_request(req_ptr, req_addr)
            raise _error_from_result(result)

        await fut

    async def _fsync_once(self) -> None:
        fut = self._loop.create_future()
        req_ptr, req_addr = _alloc_fs_request()

        def fs_callback(req_ptr):
            req_view = ctypes.cast(req_ptr, POINTER(uv_fs_req_view_t)).contents
            result = req_view.result

            try:
                if result < 0:
                    if not fut.done():
                        fut.set_exception(_error_from_result(result))
                else:
                    if not fut.done():
                        fut.set_result(None)
            finally:
                _cleanup_fs_request(req_ptr)

        cb = UV_FS_CB(fs_callback)
        _set_request_callback(req_addr, cb)

        result = uv.uv_fs_fsync(self._uv_loop, req_ptr, self._fd, cb)
        if result < 0:
            _cleanup_fs_request(req_ptr, req_addr)
            raise _error_from_result(result)

        await fut


# Define libuv structures and functions
class uv_loop_t(Structure):
    _fields_ = [("data", c_void_p)]


class uv_fs_t(Structure):
    pass


class uv_buf_t(Structure):
    _fields_ = [
        ("base", POINTER(c_char)),
        ("len", c_size_t),
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

uv.uv_fs_req_cleanup.argtypes = [POINTER(uv_fs_t)]
uv.uv_fs_req_cleanup.restype = None

uv.uv_req_size.argtypes = [c_int]
uv.uv_req_size.restype = c_size_t

uv.uv_strerror.argtypes = [c_int]
uv.uv_strerror.restype = ctypes.c_char_p

# Define callback type
UV_FS_CB = CFUNCTYPE(None, POINTER(uv_fs_t))
UV_FS_REQ_TYPE = 6

# Set up the function signature for PyCapsule_GetPointer
ctypes.pythonapi.PyCapsule_GetPointer.restype = ctypes.c_void_p
ctypes.pythonapi.PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]

class _FsRequestContext:
    __slots__ = ("req_buf", "refs", "cb")

    def __init__(self, req_buf: Any, refs: tuple[Any, ...]) -> None:
        self.req_buf = req_buf
        self.refs = refs
        self.cb: Optional[Any] = None


_pending_requests: dict[int, _FsRequestContext] = {}


def _req_addr_from_ptr(req_ptr: POINTER(uv_fs_t)) -> int:
    addr = ctypes.cast(req_ptr, c_void_p).value
    if addr is None:
        raise RuntimeError("null request pointer")
    return int(addr)


def _alloc_fs_request(*refs: Any) -> tuple[POINTER(uv_fs_t), int]:
    req_size = uv.uv_req_size(UV_FS_REQ_TYPE)
    req_buf = ctypes.create_string_buffer(req_size)
    req_ptr = ctypes.cast(req_buf, POINTER(uv_fs_t))
    req_addr = ctypes.addressof(req_buf)
    _pending_requests[req_addr] = _FsRequestContext(req_buf, refs)
    return req_ptr, req_addr


def _set_request_callback(req_addr: int, cb: UV_FS_CB) -> None:
    context = _pending_requests.get(req_addr)
    if context is None:
        raise RuntimeError("request context missing before callback setup")
    context.cb = cb


def _cleanup_fs_request(req_ptr: POINTER(uv_fs_t), req_addr: Optional[int] = None) -> None:
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

    req_ptr, req_addr = _alloc_fs_request()

    fut = loop.create_future()
    file_mode = _flags_to_mode(flags)

    def fs_callback(req_ptr):
        req_view = ctypes.cast(req_ptr, POINTER(uv_fs_req_view_t)).contents
        result = req_view.result

        try:
            if result < 0:
                if not fut.done():
                    fut.set_exception(_error_from_result(result))
            else:
                if not fut.done():
                    fut.set_result(AsyncFile(result, path, loop, file_mode))
        finally:
            _cleanup_fs_request(req_ptr)

    cb = UV_FS_CB(fs_callback)
    _set_request_callback(req_addr, cb)

    result = uv.uv_fs_open(
        uv_loop, req_ptr, path.encode("utf-8"), flags, mode, cb
    )

    if result < 0:
        _cleanup_fs_request(req_ptr, req_addr)
        fut.set_exception(_error_from_result(result))

    return fut


__all__ = ["open", "AsyncFile"]
