"""Async temporary files and directories, matching ``aiofiles.tempfile``.

``TemporaryFile`` and ``NamedTemporaryFile`` create the file once via the stdlib
``mkstemp`` (the only blocking step, run in the default executor) and then wrap
the resulting descriptor in a libuv-backed :class:`~uvfiles.async_file.AsyncFile`,
so the actual reads and writes are truly async. ``SpooledTemporaryFile`` stays in
memory until it rolls over, so it wraps the stdlib object via the executor, like
aiofiles does. Each factory returns an object usable as either
``async with`` or ``await``.
"""

import asyncio
import os
import shutil as _shutil
import tempfile as _tempfile
from typing import Any, Awaitable, Callable, Optional, Tuple

from .async_file import AsyncFile, _validate_newline
from .open import _parse_mode

__all__ = [
    "TemporaryFile",
    "NamedTemporaryFile",
    "SpooledTemporaryFile",
    "TemporaryDirectory",
]


def _resolve_mode(
    mode: str, encoding: Optional[str], errors: Optional[str], newline: Optional[str]
) -> Tuple[bool, Optional[str], Optional[str], Optional[str], str]:
    _flags, normalized, binary, _append = _parse_mode(mode)
    if binary:
        if encoding is not None or errors is not None or newline is not None:
            raise ValueError("binary mode doesn't take encoding/errors/newline")
        return binary, None, None, None, normalized
    _validate_newline(newline)
    return binary, encoding or "utf-8", errors or "strict", newline, normalized


class _AsyncTempContextManager:
    """Awaitable async context manager wrapping a one-shot resource factory.

    ``factory`` is an async callable returning ``(value, cleanup)`` where
    ``cleanup`` is an async callable (or None). ``await`` yields the value;
    ``async with`` yields the value and runs ``cleanup`` on exit.
    """

    def __init__(
        self, factory: Callable[[], Awaitable[Tuple[Any, Any]]]
    ) -> None:
        self._factory = factory
        self._cleanup: Optional[Callable[[], Any]] = None

    async def _create(self) -> Any:
        value, self._cleanup = await self._factory()
        return value

    def __await__(self) -> Any:
        return self._create().__await__()

    async def __aenter__(self) -> Any:
        return await self._create()

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._cleanup is not None:
            await self._cleanup()


class _TempAsyncFile(AsyncFile):
    """AsyncFile that unlinks its backing path when closed (delete=True case)."""

    def __init__(self, *args: Any, delete_path: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._delete_path = delete_path

    async def close(self) -> None:
        was_open = not self._closed
        await super().close()
        if was_open and self._delete_path is not None:
            try:
                await asyncio.get_running_loop().run_in_executor(
                    None, os.unlink, self._delete_path
                )
            except OSError:
                pass
            self._delete_path = None


def TemporaryFile(
    mode: str = "w+b",
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    suffix: Optional[str] = None,
    prefix: Optional[str] = None,
    dir: Optional[str] = None,
) -> _AsyncTempContextManager:
    """Create an unnamed temporary file, deleted when closed."""

    async def factory() -> Tuple[Any, Any]:
        if buffering != -1:
            raise NotImplementedError("buffering other than -1 is not supported")
        binary, enc, err, nl, normalized = _resolve_mode(mode, encoding, errors, newline)
        loop = asyncio.get_running_loop()

        def _make_anonymous() -> Tuple[int, str]:
            fd, name = _tempfile.mkstemp(suffix, prefix, dir, False)
            try:
                os.unlink(name)
            except BaseException:
                os.close(fd)
                raise
            return fd, name

        fd, name = await loop.run_in_executor(None, _make_anonymous)
        f = AsyncFile(
            fd, name, loop, normalized, binary=binary, encoding=enc, errors=err, newline=nl
        )
        return f, f.close

    return _AsyncTempContextManager(factory)


def NamedTemporaryFile(
    mode: str = "w+b",
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    suffix: Optional[str] = None,
    prefix: Optional[str] = None,
    dir: Optional[str] = None,
    delete: bool = True,
) -> _AsyncTempContextManager:
    """Create a named temporary file, deleted on close when ``delete`` is True."""

    async def factory() -> Tuple[Any, Any]:
        if buffering != -1:
            raise NotImplementedError("buffering other than -1 is not supported")
        binary, enc, err, nl, normalized = _resolve_mode(mode, encoding, errors, newline)
        loop = asyncio.get_running_loop()

        fd, name = await loop.run_in_executor(
            None, lambda: _tempfile.mkstemp(suffix, prefix, dir, False)
        )
        f = _TempAsyncFile(
            fd,
            name,
            loop,
            normalized,
            binary=binary,
            encoding=enc,
            errors=err,
            newline=nl,
            delete_path=name if delete else None,
        )
        return f, f.close

    return _AsyncTempContextManager(factory)


def TemporaryDirectory(
    suffix: Optional[str] = None,
    prefix: Optional[str] = None,
    dir: Optional[str] = None,
) -> _AsyncTempContextManager:
    """Create a temporary directory, recursively removed on context exit."""

    async def factory() -> Tuple[Any, Any]:
        loop = asyncio.get_running_loop()
        name = await loop.run_in_executor(
            None, lambda: _tempfile.mkdtemp(suffix, prefix, dir)
        )

        async def cleanup() -> None:
            await loop.run_in_executor(
                None, lambda: _shutil.rmtree(name, ignore_errors=True)
            )

        return name, cleanup

    return _AsyncTempContextManager(factory)


class _AsyncSpooledTemporaryFile:
    """Executor-backed async wrapper over stdlib SpooledTemporaryFile.

    The spooled file lives in memory until it exceeds ``max_size``, so there is
    no descriptor to drive through libuv; operations are offloaded to the default
    thread executor instead.
    """

    def __init__(self, spooled: Any, loop: asyncio.AbstractEventLoop) -> None:
        self._file = spooled
        self._loop = loop

    async def _run(self, func: Callable[..., Any], *args: Any) -> Any:
        return await self._loop.run_in_executor(None, func, *args)

    async def read(self, *args: Any) -> Any:
        return await self._run(self._file.read, *args)

    async def readline(self, *args: Any) -> Any:
        return await self._run(self._file.readline, *args)

    async def readlines(self, *args: Any) -> Any:
        return await self._run(self._file.readlines, *args)

    async def write(self, data: Any) -> int:
        return await self._run(self._file.write, data)

    async def writelines(self, lines: Any) -> None:
        return await self._run(self._file.writelines, lines)

    async def seek(self, *args: Any) -> int:
        return await self._run(self._file.seek, *args)

    async def tell(self) -> int:
        return await self._run(self._file.tell)

    async def truncate(self, *args: Any) -> int:
        return await self._run(self._file.truncate, *args)

    async def flush(self) -> None:
        return await self._run(self._file.flush)

    async def rollover(self) -> None:
        return await self._run(self._file.rollover)

    async def close(self) -> None:
        return await self._run(self._file.close)

    def fileno(self) -> int:
        return self._file.fileno()

    @property
    def closed(self) -> bool:
        return self._file.closed

    async def __aenter__(self) -> "_AsyncSpooledTemporaryFile":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()


def SpooledTemporaryFile(
    max_size: int = 0,
    mode: str = "w+b",
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    suffix: Optional[str] = None,
    prefix: Optional[str] = None,
    dir: Optional[str] = None,
) -> _AsyncTempContextManager:
    """Create an in-memory temporary file that rolls over to disk past max_size."""

    async def factory() -> Tuple[Any, Any]:
        loop = asyncio.get_running_loop()
        spooled = await loop.run_in_executor(
            None,
            lambda: _tempfile.SpooledTemporaryFile(
                max_size=max_size,
                mode=mode,
                buffering=buffering,
                encoding=encoding,
                errors=errors,
                newline=newline,
                suffix=suffix,
                prefix=prefix,
                dir=dir,
            ),
        )
        f = _AsyncSpooledTemporaryFile(spooled, loop)
        return f, f.close

    return _AsyncTempContextManager(factory)
