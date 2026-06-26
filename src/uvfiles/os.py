"""Async equivalents of a subset of the stdlib ``os`` filesystem helpers.

Mirrors ``aiofiles.os``: each coroutine runs a single ``uv_fs_*`` operation on
the running uvloop event loop. Operations whose sole output is libuv's integer
``result`` field plus ``stat`` / ``lstat`` live here; richer ones (scandir,
readlink, ...) need extra result views and are intentionally left out for now.
"""

import asyncio
import ctypes
import os
from ctypes import POINTER
from typing import Any, Callable, Optional, Union

from .uv import (
    UV_FS_CB,
    _alloc_fs_request,
    _cleanup_fs_request,
    _error_from_result,
    _get_uv_loop_ptr,
    _set_request_callback,
    uv,
    uv_fs_req_stat_view_t,
    uv_fs_req_view_t,
    uv_stat_t,
)

StrPath = Union[str, "os.PathLike[str]"]

__all__ = ["remove", "unlink", "rename", "mkdir", "rmdir", "stat", "lstat", "path"]


def _fsencode(path: StrPath) -> bytes:
    return os.fsencode(os.fspath(path))


async def _run_fs(
    call: Callable[[Any, Any, Any], int],
    on_success: Optional[Callable[[Any], Any]] = None,
) -> Any:
    """Run a single libuv fs op on the running loop and await its result.

    ``call`` receives ``(uv_loop, req_ptr, cb)`` and must invoke the matching
    ``uv_fs_*`` function. ``on_success`` is called with the raw request pointer
    when ``result >= 0`` to build the value the coroutine resolves to; when it is
    ``None`` the coroutine resolves to ``None``. Follows the same lifetime rules
    as the ``_*_once`` helpers in async_file.py.
    """
    loop = asyncio.get_running_loop()
    uv_loop = _get_uv_loop_ptr(loop)

    fut = loop.create_future()
    req_ptr, req_addr = _alloc_fs_request()

    def fs_callback(req_ptr: Any) -> None:
        req_view = ctypes.cast(req_ptr, POINTER(uv_fs_req_view_t)).contents
        result = req_view.result

        try:
            if result < 0:
                if not fut.done():
                    fut.set_exception(_error_from_result(result))
            else:
                value = None if on_success is None else on_success(req_ptr)
                if not fut.done():
                    fut.set_result(value)
        finally:
            _cleanup_fs_request(req_ptr)

    cb = UV_FS_CB(fs_callback)
    _set_request_callback(req_addr, cb)

    result = call(uv_loop, req_ptr, cb)
    if result < 0:
        _cleanup_fs_request(req_ptr, req_addr)
        raise _error_from_result(result)

    return await fut


def _build_stat_result(statbuf: uv_stat_t) -> os.stat_result:
    atime_ns = statbuf.st_atim.tv_sec * 1_000_000_000 + statbuf.st_atim.tv_nsec
    mtime_ns = statbuf.st_mtim.tv_sec * 1_000_000_000 + statbuf.st_mtim.tv_nsec
    ctime_ns = statbuf.st_ctim.tv_sec * 1_000_000_000 + statbuf.st_ctim.tv_nsec

    values = (
        int(statbuf.st_mode),
        int(statbuf.st_ino),
        int(statbuf.st_dev),
        int(statbuf.st_nlink),
        int(statbuf.st_uid),
        int(statbuf.st_gid),
        int(statbuf.st_size),
        atime_ns / 1e9,
        mtime_ns / 1e9,
        ctime_ns / 1e9,
    )
    extra = {
        "st_atime_ns": atime_ns,
        "st_mtime_ns": mtime_ns,
        "st_ctime_ns": ctime_ns,
        "st_blksize": int(statbuf.st_blksize),
        "st_blocks": int(statbuf.st_blocks),
        "st_rdev": int(statbuf.st_rdev),
        "st_birthtime": statbuf.st_birthtim.tv_sec
        + statbuf.st_birthtim.tv_nsec / 1e9,
    }
    return os.stat_result(values, extra)


def _on_stat(req_ptr: Any) -> os.stat_result:
    view = ctypes.cast(req_ptr, POINTER(uv_fs_req_stat_view_t)).contents
    return _build_stat_result(view.statbuf)


async def remove(path: StrPath) -> None:
    """Remove (delete) the file ``path``."""
    encoded = _fsencode(path)
    await _run_fs(lambda loop, req, cb: uv.uv_fs_unlink(loop, req, encoded, cb))


unlink = remove


async def rename(src: StrPath, dst: StrPath) -> None:
    """Rename the file or directory ``src`` to ``dst``."""
    src_encoded = _fsencode(src)
    dst_encoded = _fsencode(dst)
    await _run_fs(
        lambda loop, req, cb: uv.uv_fs_rename(loop, req, src_encoded, dst_encoded, cb)
    )


async def mkdir(path: StrPath, mode: int = 0o777) -> None:
    """Create a directory named ``path`` with numeric ``mode``."""
    encoded = _fsencode(path)
    await _run_fs(lambda loop, req, cb: uv.uv_fs_mkdir(loop, req, encoded, mode, cb))


async def rmdir(path: StrPath) -> None:
    """Remove (delete) the directory ``path``."""
    encoded = _fsencode(path)
    await _run_fs(lambda loop, req, cb: uv.uv_fs_rmdir(loop, req, encoded, cb))


async def stat(path: StrPath) -> os.stat_result:
    """Return an ``os.stat_result`` for ``path``, following symlinks."""
    encoded = _fsencode(path)
    return await _run_fs(
        lambda loop, req, cb: uv.uv_fs_stat(loop, req, encoded, cb), _on_stat
    )


async def lstat(path: StrPath) -> os.stat_result:
    """Like :func:`stat`, but do not follow symlinks."""
    encoded = _fsencode(path)
    return await _run_fs(
        lambda loop, req, cb: uv.uv_fs_lstat(loop, req, encoded, cb), _on_stat
    )


# Imported last so uvfiles.os.stat / lstat are already defined when ospath binds
# them, avoiding a circular import. Exposes uvfiles.os.path like aiofiles.os.path.
from . import ospath as path  # noqa: E402
