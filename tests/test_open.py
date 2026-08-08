import asyncio
import errno
import os

import pytest

import uvfiles
from uvfiles import AsyncFile


def test_open_requires_uvloop_loop(tmp_path):
    path = tmp_path / "requires_uvloop.bin"
    path.write_bytes(b"x")

    non_uvloop = asyncio.DefaultEventLoopPolicy().new_event_loop()
    try:
        with pytest.raises(RuntimeError, match="requires a uvloop event loop"):
            uvfiles.open(str(path), os.O_RDONLY, loop=non_uvloop)
    finally:
        non_uvloop.close()


@pytest.mark.asyncio
async def test_async_open_and_close(tmp_path):
    path = tmp_path / "open_close.bin"
    path.write_bytes(b"hello")

    f = await uvfiles.open(str(path), os.O_RDONLY)
    assert isinstance(f, AsyncFile)
    assert f.fileno() > 2  # Should not be stdin/stdout/stderr
    assert f.name == str(path)
    assert f.mode == "r"
    assert f.closed is False
    assert await f.readable() is True
    assert await f.writable() is False

    await f.close()
    assert f.closed is True

    await f.close()  # idempotent


@pytest.mark.asyncio
async def test_open_mode_string_text_roundtrip(tmp_path):
    path = tmp_path / "text_roundtrip.txt"

    f = await uvfiles.open(str(path), "w+")
    written = await f.write("abc\n123")
    assert written == 7
    await f.seek(0)
    assert await f.read() == "abc\n123"
    await f.close()


@pytest.mark.asyncio
async def test_open_default_mode_is_text(tmp_path):
    path = tmp_path / "default_text.txt"
    path.write_text("hello", encoding="utf-8")

    f = await uvfiles.open(str(path))
    data = await f.read()
    assert isinstance(data, str)
    assert data == "hello"
    await f.close()


@pytest.mark.asyncio
async def test_open_accepts_path_like(tmp_path):
    path = tmp_path / "path_like.txt"
    path.write_text("hello", encoding="utf-8")

    f = await uvfiles.open(path)
    assert await f.read() == "hello"
    assert f.name == str(path)
    await f.close()


@pytest.mark.asyncio
async def test_open_int_flags_keeps_binary_behavior(tmp_path):
    path = tmp_path / "flags_binary.bin"
    path.write_bytes(b"\xe4\xb8\xad")

    f = await uvfiles.open(str(path), os.O_RDONLY)
    data = await f.read()
    assert isinstance(data, bytes)
    assert data == b"\xe4\xb8\xad"
    await f.close()


@pytest.mark.asyncio
async def test_async_open_alias(tmp_path):
    path = tmp_path / "alias.txt"
    f = await uvfiles.async_open(str(path), "w+")
    await f.write("alias")
    await f.seek(0)
    assert await f.read() == "alias"
    await f.close()


def test_open_buffering_not_supported(tmp_path):
    path = tmp_path / "buffering.txt"
    path.write_text("hello", encoding="utf-8")

    with pytest.raises(NotImplementedError):
        uvfiles.open(str(path), "r", buffering=1)


@pytest.mark.asyncio
async def test_open_missing_file_raises_file_not_found(tmp_path):
    path = tmp_path / "missing.txt"

    with pytest.raises(FileNotFoundError) as excinfo:
        await uvfiles.open(str(path), "r")

    assert excinfo.value.errno == errno.ENOENT
    assert excinfo.value.filename == str(path)


@pytest.mark.asyncio
async def test_open_exclusive_existing_raises_file_exists(tmp_path):
    path = tmp_path / "exists.txt"
    path.write_bytes(b"x")

    with pytest.raises(FileExistsError) as excinfo:
        await uvfiles.open(str(path), "x")

    assert excinfo.value.errno == errno.EEXIST
    assert excinfo.value.filename == str(path)


@pytest.mark.asyncio
async def test_open_directory_raises_is_a_directory(tmp_path):
    with pytest.raises(IsADirectoryError):
        await uvfiles.open(str(tmp_path), "w")
