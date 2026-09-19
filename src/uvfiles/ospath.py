"""Async equivalents of a subset of stdlib ``os.path``, matching ``aiofiles.os.path``.

The metadata predicates (``exists`` / ``isfile`` / ... / ``samefile``) run on
libuv through :mod:`uvfiles.os` ``stat`` / ``lstat`` so they are genuinely
non-blocking, as do ``sameopenfile`` and ``ismount``. ``abspath`` delegates to
the stdlib: it is pure path logic that at most reads the cwd, which involves no
filesystem I/O.
"""

import os
import stat as _stat
from typing import Union

from . import os as _aos

StrPath = Union[str, "os.PathLike[str]"]

__all__ = [
    "exists",
    "isfile",
    "isdir",
    "islink",
    "getsize",
    "getmtime",
    "getatime",
    "getctime",
    "samefile",
    "sameopenfile",
    "abspath",
    "ismount",
]


async def exists(path: StrPath) -> bool:
    """Return True if ``path`` refers to an existing path (following symlinks)."""
    try:
        await _aos.stat(path)
    except (OSError, ValueError):
        return False
    return True


async def isfile(path: StrPath) -> bool:
    """Return True if ``path`` is an existing regular file (following symlinks)."""
    try:
        st = await _aos.stat(path)
    except (OSError, ValueError):
        return False
    return _stat.S_ISREG(st.st_mode)


async def isdir(path: StrPath) -> bool:
    """Return True if ``path`` is an existing directory (following symlinks)."""
    try:
        st = await _aos.stat(path)
    except (OSError, ValueError):
        return False
    return _stat.S_ISDIR(st.st_mode)


async def islink(path: StrPath) -> bool:
    """Return True if ``path`` refers to an existing symbolic link."""
    try:
        st = await _aos.lstat(path)
    except (OSError, ValueError):
        return False
    return _stat.S_ISLNK(st.st_mode)


async def getsize(path: StrPath) -> int:
    """Return the size of ``path`` in bytes."""
    st = await _aos.stat(path)
    return st.st_size


async def getmtime(path: StrPath) -> float:
    """Return the last modification time of ``path``."""
    st = await _aos.stat(path)
    return st.st_mtime


async def getatime(path: StrPath) -> float:
    """Return the last access time of ``path``."""
    st = await _aos.stat(path)
    return st.st_atime


async def getctime(path: StrPath) -> float:
    """Return the metadata change time of ``path``."""
    st = await _aos.stat(path)
    return st.st_ctime


async def samefile(path1: StrPath, path2: StrPath) -> bool:
    """Return True if both paths refer to the same file."""
    s1 = await _aos.stat(path1)
    s2 = await _aos.stat(path2)
    return os.path.samestat(s1, s2)


async def sameopenfile(fd1: int, fd2: int) -> bool:
    """Return True if the two open file descriptors refer to the same file."""
    s1 = await _aos._fstat(fd1)
    s2 = await _aos._fstat(fd2)
    return os.path.samestat(s1, s2)


async def abspath(path: StrPath) -> str:
    """Return a normalized absolutized version of ``path``."""
    # Pure path normalization (only reads cwd); no filesystem I/O to offload.
    return os.path.abspath(path)


async def ismount(path: StrPath) -> bool:
    """Return True if ``path`` is a mount point."""
    # Same checks as posixpath.ismount, with the syscalls routed through libuv.
    try:
        s1 = await _aos.lstat(path)
    except (OSError, ValueError):
        return False
    if _stat.S_ISLNK(s1.st_mode):
        return False

    # ``path`` exists and is not a symlink, so the parent of its canonical form
    # is what the stdlib's realpath(join(path, "..")) resolves to.
    try:
        parent = os.path.dirname(await _aos._realpath(path))
        s2 = await _aos.lstat(parent)
    except (OSError, ValueError):
        return False

    if s1.st_dev != s2.st_dev:
        return True  # path/.. on a different device as path
    return s1.st_ino == s2.st_ino  # path/.. is the same i-node as path
