"""Async equivalents of a subset of the stdlib ``os`` filesystem helpers.

Mirrors ``aiofiles.os``: each coroutine runs a single ``uv_fs_*`` operation on
the running uvloop event loop. Operations whose sole output is libuv's integer
``result`` field plus ``stat`` / ``lstat`` live here; richer ones (scandir,
readlink, ...) need extra result views and are intentionally left out for now.
"""

import asyncio
import ctypes
import os
import stat as _filestat
from ctypes import POINTER
from typing import Any, Callable, List, Optional, Union

from .uv import (
    UV_DIRENT_DIR,
    UV_DIRENT_FILE,
    UV_DIRENT_LINK,
    UV_DIRENT_UNKNOWN,
    UV_FS_CB,
    _alloc_fs_request,
    _cleanup_fs_request,
    _error_from_result,
    _get_uv_loop_ptr,
    _set_request_callback,
    uv,
    uv_dirent_t,
    uv_fs_req_ptr_view_t,
    uv_fs_req_stat_view_t,
    uv_fs_req_view_t,
    uv_stat_t,
)

StrPath = Union[str, "os.PathLike[str]"]

__all__ = [
    "remove",
    "unlink",
    "rename",
    "replace",
    "renames",
    "mkdir",
    "makedirs",
    "rmdir",
    "removedirs",
    "stat",
    "lstat",
    "access",
    "link",
    "symlink",
    "readlink",
    "listdir",
    "scandir",
    "getcwd",
    "DirEntry",
    "path",
]


def _fsencode(path: StrPath) -> bytes:
    return os.fsencode(os.fspath(path))


async def _run_fs(
    call: Callable[[Any, Any, Any], int],
    on_success: Optional[Callable[[Any], Any]] = None,
    *,
    raise_on_error: bool = True,
) -> Any:
    """Run a single libuv fs op on the running loop and await its result.

    ``call`` receives ``(uv_loop, req_ptr, cb)`` and must invoke the matching
    ``uv_fs_*`` function. ``on_success`` is called with the raw request pointer
    when ``result >= 0`` to build the value the coroutine resolves to; when it is
    ``None`` the coroutine resolves to ``None``. When ``raise_on_error`` is False
    a negative ``result`` resolves to that integer instead of raising (used by
    ``access``, which reports failure as a bool). Follows the same lifetime rules
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
                    if raise_on_error:
                        fut.set_exception(_error_from_result(result))
                    else:
                        fut.set_result(int(result))
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
        if raise_on_error:
            raise _error_from_result(result)
        return int(result)

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


async def access(path: StrPath, mode: int) -> bool:
    """Return True if the calling user can access ``path`` with the given mode."""
    encoded = _fsencode(path)
    result = await _run_fs(
        lambda loop, req, cb: uv.uv_fs_access(loop, req, encoded, mode, cb),
        lambda req_ptr: True,
        raise_on_error=False,
    )
    return result is True


async def link(src: StrPath, dst: StrPath) -> None:
    """Create a hard link ``dst`` pointing to ``src``."""
    src_encoded = _fsencode(src)
    dst_encoded = _fsencode(dst)
    await _run_fs(
        lambda loop, req, cb: uv.uv_fs_link(loop, req, src_encoded, dst_encoded, cb)
    )


async def symlink(
    src: StrPath, dst: StrPath, target_is_directory: bool = False
) -> None:
    """Create a symbolic link ``dst`` pointing to ``src``.

    ``target_is_directory`` is accepted for ``os.symlink`` compatibility and only
    has an effect on Windows, which uvloop does not support.
    """
    src_encoded = _fsencode(src)
    dst_encoded = _fsencode(dst)
    await _run_fs(
        lambda loop, req, cb: uv.uv_fs_symlink(
            loop, req, src_encoded, dst_encoded, 0, cb
        )
    )


def _on_readlink(req_ptr: Any) -> str:
    view = ctypes.cast(req_ptr, POINTER(uv_fs_req_ptr_view_t)).contents
    if not view.ptr:
        return ""
    target = ctypes.cast(view.ptr, ctypes.c_char_p).value
    return os.fsdecode(target) if target is not None else ""


async def readlink(path: StrPath) -> str:
    """Return the path the symbolic link ``path`` points to."""
    encoded = _fsencode(path)
    return await _run_fs(
        lambda loop, req, cb: uv.uv_fs_readlink(loop, req, encoded, cb), _on_readlink
    )


async def replace(src: StrPath, dst: StrPath) -> None:
    """Rename ``src`` to ``dst``, atomically replacing an existing ``dst``."""
    # POSIX rename(2) -- which uv_fs_rename wraps -- already overwrites the
    # destination, so this matches os.replace semantics on the platforms uvloop
    # supports.
    await rename(src, dst)


async def getcwd() -> str:
    """Return the current working directory."""
    # Pure metadata lookup with no blocking I/O; no libuv request needed.
    return os.getcwd()


class DirEntry:
    """Lightweight ``os.DirEntry``-compatible entry produced by :func:`scandir`.

    The file type cached from the directory read answers ``is_dir`` / ``is_file``
    / ``is_symlink`` without a syscall in the common case. When the type is
    unknown, or a symlink must be resolved with ``follow_symlinks=True``, it falls
    back to a synchronous ``os.stat`` / ``os.lstat`` -- exactly like
    ``os.DirEntry``, whose methods (and ``stat``) are likewise synchronous for
    drop-in compatibility.
    """

    __slots__ = ("name", "path", "_d_type", "_stat_cache", "_lstat_cache")

    def __init__(self, name: str, entry_path: str, d_type: int) -> None:
        self.name = name
        self.path = entry_path
        self._d_type = d_type
        self._stat_cache: Optional[os.stat_result] = None
        self._lstat_cache: Optional[os.stat_result] = None

    def __fspath__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        return f"<DirEntry {self.name!r}>"

    def inode(self) -> int:
        return self._get_lstat().st_ino

    def stat(self, *, follow_symlinks: bool = True) -> os.stat_result:
        return self._get_stat() if follow_symlinks else self._get_lstat()

    def is_symlink(self) -> bool:
        if self._d_type == UV_DIRENT_LINK:
            return True
        if self._d_type != UV_DIRENT_UNKNOWN:
            return False
        try:
            return _filestat.S_ISLNK(self._get_lstat().st_mode)
        except OSError:
            return False

    def is_dir(self, *, follow_symlinks: bool = True) -> bool:
        return self._is_type(_filestat.S_ISDIR, UV_DIRENT_DIR, follow_symlinks)

    def is_file(self, *, follow_symlinks: bool = True) -> bool:
        return self._is_type(_filestat.S_ISREG, UV_DIRENT_FILE, follow_symlinks)

    def _is_type(
        self, st_check: Callable[[int], bool], dirent_type: int, follow_symlinks: bool
    ) -> bool:
        if self._d_type == dirent_type:
            return True
        # A definitive non-matching type (and not a symlink to resolve) is final.
        if self._d_type not in (UV_DIRENT_UNKNOWN, UV_DIRENT_LINK):
            return False
        try:
            target = self._get_stat() if follow_symlinks else self._get_lstat()
        except OSError:
            return False
        return st_check(target.st_mode)

    def _get_stat(self) -> os.stat_result:
        if self._stat_cache is None:
            self._stat_cache = os.stat(self.path)
        return self._stat_cache

    def _get_lstat(self) -> os.stat_result:
        if self._lstat_cache is None:
            self._lstat_cache = os.lstat(self.path)
        return self._lstat_cache


class ScandirResult(List[DirEntry]):
    """List of :class:`DirEntry` that is also a (no-op) context manager.

    libuv reads the whole directory eagerly, so unlike ``os.scandir`` there is no
    open handle to close; ``close`` / ``with`` / ``async with`` exist only for
    drop-in compatibility.
    """

    def __enter__(self) -> "ScandirResult":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    async def __aenter__(self) -> "ScandirResult":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    def close(self) -> None:
        pass


def _iter_dirents(req_ptr: Any) -> Any:
    ent = uv_dirent_t()
    while uv.uv_fs_scandir_next(req_ptr, ctypes.byref(ent)) == 0:
        yield os.fsdecode(ent.name), int(ent.type)


async def listdir(path: StrPath = ".") -> List[str]:
    """Return a list of the entry names in directory ``path``.

    Excludes the ``.`` and ``..`` entries, matching ``os.listdir``.
    """
    encoded = _fsencode(path)
    return await _run_fs(
        lambda loop, req, cb: uv.uv_fs_scandir(loop, req, encoded, 0, cb),
        lambda req_ptr: [name for name, _type in _iter_dirents(req_ptr)],
    )


async def scandir(path: StrPath = ".") -> ScandirResult:
    """Return :class:`DirEntry` objects for the entries in directory ``path``."""
    base = os.fspath(path)
    encoded = _fsencode(base)

    def on_scandir(req_ptr: Any) -> ScandirResult:
        result = ScandirResult()
        for name, d_type in _iter_dirents(req_ptr):
            result.append(DirEntry(name, os.path.join(base, name), d_type))
        return result

    return await _run_fs(
        lambda loop, req, cb: uv.uv_fs_scandir(loop, req, encoded, 0, cb),
        on_scandir,
    )


async def makedirs(
    name: StrPath, mode: int = 0o777, exist_ok: bool = False
) -> None:
    """Recursive directory creation, like ``os.makedirs``."""
    name = os.fspath(name)
    head, tail = os.path.split(name)
    if not tail:
        head, tail = os.path.split(head)
    if head and tail and not await path.exists(head):
        try:
            await makedirs(head, mode, exist_ok=exist_ok)
        except FileExistsError:
            pass
        if tail == os.curdir:
            return
    try:
        await mkdir(name, mode)
    except OSError:
        if not exist_ok or not await path.isdir(name):
            raise


async def removedirs(name: StrPath) -> None:
    """Remove ``name`` then prune now-empty parent directories, like os.removedirs."""
    name = os.fspath(name)
    await rmdir(name)
    head, tail = os.path.split(name)
    if not tail:
        head, tail = os.path.split(head)
    while head and tail:
        try:
            await rmdir(head)
        except OSError:
            break
        head, tail = os.path.split(head)


async def renames(old: StrPath, new: StrPath) -> None:
    """Recursive rename: create missing parents of ``new``, then prune ``old``'s."""
    old = os.fspath(old)
    new = os.fspath(new)
    head, tail = os.path.split(new)
    if head and tail and not await path.exists(head):
        await makedirs(head)
    await rename(old, new)
    head, tail = os.path.split(old)
    if head and tail:
        try:
            await removedirs(head)
        except OSError:
            pass


# Imported last so uvfiles.os.stat / lstat are already defined when ospath binds
# them, avoiding a circular import. Exposes uvfiles.os.path like aiofiles.os.path.
from . import ospath as path  # noqa: E402
