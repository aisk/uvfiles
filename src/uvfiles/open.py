import asyncio
import ctypes
import os
from ctypes import POINTER
from typing import Optional

from .async_file import AsyncFile, _validate_newline
from .uv import (
    UV_FS_CB,
    _alloc_fs_request,
    _cleanup_fs_request,
    _error_from_result,
    _get_uv_loop_ptr,
    _set_request_callback,
    uv,
    uv_fs_req_view_t,
)


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


def _parse_mode(mode: str) -> tuple[int, str, bool, bool]:
    if not isinstance(mode, str):
        raise TypeError("mode must be str")
    if not mode:
        raise ValueError("must have exactly one of create/read/write/append mode")

    base_chars = {"r", "w", "a", "x"}
    base = mode[0]
    if base not in base_chars:
        raise ValueError("must have exactly one of create/read/write/append mode")

    plus = False
    binary = False
    text = False

    for ch in mode[1:]:
        if ch == "+":
            if plus:
                raise ValueError("invalid mode: duplicated '+'")
            plus = True
            continue
        if ch == "b":
            if binary:
                raise ValueError("invalid mode: duplicated 'b'")
            binary = True
            continue
        if ch == "t":
            if text:
                raise ValueError("invalid mode: duplicated 't'")
            text = True
            continue
        raise ValueError(f"invalid mode: {mode!r}")

    if binary and text:
        raise ValueError("can't have text and binary mode at once")

    if base == "r":
        flags = os.O_RDONLY
    elif base == "w":
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    elif base == "a":
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
    else:  # base == "x"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL

    if plus:
        flags = (flags & ~os.O_WRONLY) | os.O_RDWR

    normalized_mode = base
    if plus:
        normalized_mode += "+"
    if binary:
        normalized_mode += "b"
    elif text:
        normalized_mode += "t"

    return flags, normalized_mode, binary, base == "a"


def open(
    path: str,
    mode_or_flags: str | int = "r",
    mode: int = 0o644,
    *,
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    loop: Optional[asyncio.AbstractEventLoop] = None,
) -> asyncio.Future[AsyncFile]:
    if not isinstance(path, str):
        raise TypeError("path must be str")

    if not isinstance(mode, int):
        raise TypeError("mode must be int")

    if buffering != -1:
        raise NotImplementedError("buffering other than -1 is not supported")

    if loop is None:
        loop = asyncio.get_running_loop()

    uv_loop = _get_uv_loop_ptr(loop)

    req_ptr, req_addr = _alloc_fs_request()

    fut = loop.create_future()
    resolved_encoding: Optional[str] = None
    resolved_errors: Optional[str] = None
    resolved_newline: Optional[str] = None

    if isinstance(mode_or_flags, int):
        flags = mode_or_flags
        if encoding is not None or errors is not None or newline is not None:
            raise ValueError("encoding/errors/newline are only supported in text mode")
        file_mode = _flags_to_mode(flags)
        binary = True
        append = bool(flags & os.O_APPEND)
    else:
        flags, file_mode, binary, append = _parse_mode(mode_or_flags)
        if binary:
            if encoding is not None or errors is not None or newline is not None:
                raise ValueError("binary mode doesn't take encoding/errors/newline")
        else:
            _validate_newline(newline)
            resolved_encoding = encoding or "utf-8"
            resolved_errors = errors or "strict"
            resolved_newline = newline

    def fs_callback(req_ptr):
        req_view = ctypes.cast(req_ptr, POINTER(uv_fs_req_view_t)).contents
        result = req_view.result

        try:
            if result < 0:
                if not fut.done():
                    fut.set_exception(_error_from_result(result))
            else:
                if not fut.done():
                    fut.set_result(
                        AsyncFile(
                            result,
                            path,
                            loop,
                            file_mode,
                            binary=binary,
                            encoding=resolved_encoding,
                            errors=resolved_errors,
                            newline=resolved_newline,
                            append=append,
                        )
                    )
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


async_open = open
