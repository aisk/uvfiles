#!/usr/bin/env python3

import asyncio
import os

import pytest

import uvloop
import uvfiles
from uvfiles import AsyncFile

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


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
    assert f.readable() is True
    assert f.writable() is False

    await f.close()
    assert f.closed is True

    await f.close()  # idempotent


@pytest.mark.asyncio
async def test_async_write_and_read_roundtrip(tmp_path):
    path = tmp_path / "roundtrip.bin"
    f = await uvfiles.open(str(path), os.O_CREAT | os.O_RDWR | os.O_TRUNC)

    written = await f.write(b"abc\n123")
    assert written == 7
    assert f.tell() == 7

    assert f.seek(0) == 0
    content = await f.read()
    assert content == b"abc\n123"
    assert f.tell() == 7

    await f.close()


@pytest.mark.asyncio
async def test_read_with_size(tmp_path):
    path = tmp_path / "read_size.bin"
    path.write_bytes(b"abcdef")

    f = await uvfiles.open(str(path), os.O_RDONLY)
    assert await f.read(3) == b"abc"
    assert f.tell() == 3
    assert await f.read(2) == b"de"
    assert f.tell() == 5
    assert await f.read(10) == b"f"
    assert f.tell() == 6

    await f.close()


@pytest.mark.asyncio
async def test_seek_tell_whence(tmp_path):
    path = tmp_path / "seek_tell.bin"
    path.write_bytes(b"0123456789")

    f = await uvfiles.open(str(path), os.O_RDONLY)
    assert f.seek(2) == 2
    assert f.tell() == 2
    assert f.seek(3, os.SEEK_CUR) == 5
    assert f.seek(-1, os.SEEK_END) == 9
    assert f.tell() == 9

    with pytest.raises(ValueError):
        f.seek(-11, os.SEEK_END)
    with pytest.raises(ValueError):
        f.seek(0, 12345)

    await f.close()


@pytest.mark.asyncio
async def test_closed_file_operations(tmp_path):
    path = tmp_path / "closed.bin"
    path.write_bytes(b"data")

    f = await uvfiles.open(str(path), os.O_RDWR)
    await f.close()

    with pytest.raises(ValueError):
        f.fileno()
    with pytest.raises(ValueError):
        f.seekable()
    with pytest.raises(ValueError):
        f.seek(0)
    with pytest.raises(ValueError):
        f.tell()
    with pytest.raises(ValueError):
        await f.read()
    with pytest.raises(ValueError):
        await f.write(b"x")


@pytest.mark.asyncio
async def test_async_context_manager(tmp_path):
    path = tmp_path / "context.bin"

    async with await uvfiles.open(str(path), os.O_CREAT | os.O_RDWR | os.O_TRUNC) as f:
        assert f.closed is False
        await f.write(b"ctx")

    assert f.closed is True
    assert path.read_bytes() == b"ctx"
