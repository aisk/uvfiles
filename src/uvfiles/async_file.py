import asyncio
import codecs
import ctypes
import os
from ctypes import POINTER, c_char
from typing import Any, Iterator, List, Optional

from .uv import (
    UV_FS_CB,
    UV_TTY_HANDLE_TYPE,
    _alloc_fs_request,
    _cleanup_fs_request,
    _error_from_result,
    _get_uv_loop_ptr,
    _set_request_callback,
    uv,
    uv_buf_t,
    uv_fs_req_stat_view_t,
    uv_fs_req_view_t,
)


def _validate_newline(newline: Optional[str]) -> None:
    if newline not in (None, "", "\n", "\r", "\r\n"):
        raise ValueError("illegal newline value")


class AsyncFile:
    """Async file object compatible with Python's built-in file object interface."""

    def __init__(
        self,
        fd: int,
        name: str,
        loop: asyncio.AbstractEventLoop,
        mode: str = "r",
        *,
        binary: bool = True,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        append: bool = False,
    ) -> None:
        if not binary:
            if encoding is None:
                encoding = "utf-8"
            if errors is None:
                errors = "strict"
            _validate_newline(newline)

        self._fd = fd
        self._name = name
        self._loop = loop
        self._uv_loop = _get_uv_loop_ptr(loop)
        self._mode = mode
        self._binary = binary
        self._encoding = encoding if not binary else None
        self._errors = errors if not binary else None
        self._newline = newline if not binary else None
        self._pos = os.fstat(fd).st_size if append else 0
        self._closed = False
        self._read_buffer = bytearray()
        self._text_buffer = ""
        self._text_pending_cr = False
        self._text_decoder_finalized = False
        self._text_decoder = self._make_text_decoder()

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

    @property
    def encoding(self) -> Optional[str]:
        """The text encoding."""
        return self._encoding

    @property
    def errors(self) -> Optional[str]:
        """The text error strategy."""
        return self._errors

    def fileno(self) -> int:
        """Return the file descriptor."""
        if self._closed:
            raise ValueError("I/O operation on closed file")
        return self._fd

    async def readable(self) -> bool:
        """Return True if the file is readable."""
        self._ensure_open()
        self._ensure_loop()
        return "r" in self._mode or "+" in self._mode

    async def writable(self) -> bool:
        """Return True if the file is writable."""
        self._ensure_open()
        self._ensure_loop()
        return "w" in self._mode or "a" in self._mode or "+" in self._mode

    async def seekable(self) -> bool:
        """Return True if the file is seekable."""
        self._ensure_open()
        self._ensure_loop()
        return True

    async def isatty(self) -> bool:
        """Return True if the file is connected to a TTY device."""
        self._ensure_open()
        self._ensure_loop()
        return uv.uv_guess_handle(self._fd) == UV_TTY_HANDLE_TYPE

    async def read(self, size: int = -1) -> Any:
        """Read and return up to size bytes."""
        self._ensure_open()
        self._ensure_loop()

        if size == 0:
            return b"" if self._binary else ""

        if self._binary:
            return await self._read_binary(size)
        return await self._read_text(size)

    async def readline(self, size: int = -1) -> Any:
        """Read and return one line from the file."""
        self._ensure_open()
        self._ensure_loop()

        if size == 0:
            return b"" if self._binary else ""

        if self._binary:
            return await self._readline_binary(size)
        return await self._readline_text(size)

    async def readlines(self, hint: int = -1) -> List[Any]:
        """Read and return a list of lines from the file."""
        self._ensure_open()
        self._ensure_loop()

        lines = []
        total = 0
        eof = b"" if self._binary else ""

        while True:
            line = await self.readline()
            if line == eof:
                break

            lines.append(line)
            total += len(line)

            if hint > 0 and total >= hint:
                break

        return lines

    async def write(self, data: Any) -> int:
        """Write data to the file."""
        self._ensure_open()
        self._ensure_loop()

        if self._binary:
            if not isinstance(data, (bytes, bytearray, memoryview)):
                raise TypeError(
                    f"a bytes-like object is required, not {type(data).__name__}"
                )
            payload = bytes(data)
            result_size = None
        else:
            if not isinstance(data, str):
                raise TypeError(f"write() argument must be str, not {type(data).__name__}")
            result_size = len(data)
            payload = self._translate_write_newlines(data).encode(
                self._encoding or "utf-8", self._errors or "strict"
            )

        if not payload:
            return 0

        written = await self._write_once(payload, self._pos)
        self._pos += written
        if written:
            self._reset_read_state()
        return written if result_size is None else result_size

    async def writelines(self, lines: List[Any]) -> None:
        """Write a list of lines to the file."""
        self._ensure_open()
        self._ensure_loop()

        for line in lines:
            await self.write(line)

    async def readinto(self, buffer: Any) -> int:
        """Read bytes into a writable buffer and return the byte count."""
        self._ensure_open()
        self._ensure_loop()

        if not self._binary:
            raise TypeError("readinto() is only supported in binary mode")

        view = memoryview(buffer)
        if view.readonly:
            raise TypeError("readinto() argument must be read-write bytes-like object")

        byte_view = view.cast("B")
        data = await self.read(len(byte_view))
        size = len(data)
        byte_view[:size] = data
        return size

    async def seek(self, offset: int, whence: int = 0) -> int:
        """Change the stream position."""
        self._ensure_open()
        self._ensure_loop()

        if whence == os.SEEK_SET:
            new_pos = offset
        elif whence == os.SEEK_CUR:
            new_pos = self._pos + offset
        elif whence == os.SEEK_END:
            new_pos = await self._fstat_once() + offset
        else:
            raise ValueError("invalid whence")

        if new_pos < 0:
            raise ValueError("negative seek position")

        self._pos = new_pos
        self._reset_read_state()
        return self._pos

    async def tell(self) -> int:
        """Return the current stream position."""
        self._ensure_open()
        self._ensure_loop()
        return self._pos

    async def truncate(self, size: Optional[int] = None) -> int:
        """Truncate the file to at most size bytes."""
        self._ensure_open()
        self._ensure_loop()

        target_size = self._pos if size is None else size
        if target_size < 0:
            raise ValueError("negative size value")

        await self._truncate_once(target_size)
        self._reset_read_state()

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

    async def __anext__(self) -> Any:
        line = await self.readline()
        if line == (b"" if self._binary else ""):
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

    def _make_text_decoder(self) -> Optional[Any]:
        if self._binary:
            return None

        decoder_cls = codecs.getincrementaldecoder(self._encoding or "utf-8")
        return decoder_cls(errors=self._errors or "strict")

    def _reset_read_state(self) -> None:
        self._read_buffer.clear()
        self._text_buffer = ""
        self._text_pending_cr = False
        self._text_decoder_finalized = False
        self._text_decoder = self._make_text_decoder()

    def _consume_from_read_buffer(self, size: int) -> bytes:
        available = min(size, len(self._read_buffer))
        if available <= 0:
            return b""

        data = bytes(self._read_buffer[:available])
        del self._read_buffer[:available]
        self._pos += available
        return data

    def _consume_text_buffer(self, size: int) -> str:
        available = min(size, len(self._text_buffer))
        if available <= 0:
            return ""

        data = self._text_buffer[:available]
        self._text_buffer = self._text_buffer[available:]
        return data

    async def _read_binary(self, size: int = -1) -> bytes:
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

    async def _readline_binary(self, size: int = -1) -> bytes:
        limit = None if size < 0 else size
        chunks: List[bytes] = []
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

    async def _read_text(self, size: int = -1) -> str:
        if size < 0:
            while True:
                data = await self._read_once(64 * 1024, self._pos)
                if not data:
                    self._finalize_text_decoder()
                    break
                self._pos += len(data)
                self._append_decoded_text(data)

            return self._consume_text_buffer(len(self._text_buffer))

        while len(self._text_buffer) < size:
            data = await self._read_once(8 * 1024, self._pos)
            if not data:
                self._finalize_text_decoder()
                break
            self._pos += len(data)
            self._append_decoded_text(data)

        return self._consume_text_buffer(size)

    async def _readline_text(self, size: int = -1) -> str:
        limit = None if size < 0 else size
        chunks: List[str] = []
        total = 0

        while True:
            if limit is not None and total >= limit:
                return "".join(chunks)

            ch = await self._read_text(1)
            if ch == "":
                return "".join(chunks)

            chunks.append(ch)
            total += 1

            if limit is not None and total >= limit:
                return "".join(chunks)

            if self._newline is None or self._newline == "\n":
                if ch == "\n":
                    return "".join(chunks)
                continue

            if self._newline == "\r":
                if ch == "\r":
                    return "".join(chunks)
                continue

            if self._newline == "\r\n":
                if len(chunks) >= 2 and chunks[-2] == "\r" and ch == "\n":
                    return "".join(chunks)
                continue

            if self._newline == "":
                if ch == "\n":
                    return "".join(chunks)
                if ch == "\r":
                    next_ch = await self._read_text(1)
                    if next_ch == "":
                        return "".join(chunks)
                    if next_ch == "\n":
                        chunks.append(next_ch)
                        total += 1
                    else:
                        self._text_buffer = next_ch + self._text_buffer
                    return "".join(chunks)

    def _append_decoded_text(self, data: bytes, *, final: bool = False) -> None:
        if self._text_decoder is None:
            return

        decoded = self._text_decoder.decode(data, final=final)
        translated = self._translate_read_newlines(decoded, final=final)
        if translated:
            self._text_buffer += translated

    def _finalize_text_decoder(self) -> None:
        if self._text_decoder_finalized:
            return

        self._text_decoder_finalized = True
        self._append_decoded_text(b"", final=True)

    def _translate_read_newlines(self, text: str, *, final: bool) -> str:
        if self._newline is not None:
            return text

        if self._text_pending_cr:
            text = "\r" + text
            self._text_pending_cr = False

        if not final and text.endswith("\r"):
            self._text_pending_cr = True
            text = text[:-1]

        text = text.replace("\r\n", "\n").replace("\r", "\n")

        if final and self._text_pending_cr:
            text += "\n"
            self._text_pending_cr = False

        return text

    def _translate_write_newlines(self, text: str) -> str:
        if self._newline is None:
            return text.replace("\n", os.linesep)
        if self._newline in ("", "\n"):
            return text
        return text.replace("\n", self._newline)

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

    async def _fstat_once(self) -> int:
        fut = self._loop.create_future()
        req_ptr, req_addr = _alloc_fs_request()

        def fs_callback(req_ptr):
            req_view = ctypes.cast(req_ptr, POINTER(uv_fs_req_stat_view_t)).contents
            result = req_view.result

            try:
                if result < 0:
                    if not fut.done():
                        fut.set_exception(_error_from_result(result))
                else:
                    if not fut.done():
                        fut.set_result(int(req_view.statbuf.st_size))
            finally:
                _cleanup_fs_request(req_ptr)

        cb = UV_FS_CB(fs_callback)
        _set_request_callback(req_addr, cb)

        result = uv.uv_fs_fstat(self._uv_loop, req_ptr, self._fd, cb)
        if result < 0:
            _cleanup_fs_request(req_ptr, req_addr)
            raise _error_from_result(result)

        return await fut
