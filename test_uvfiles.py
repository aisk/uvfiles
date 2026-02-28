#!/usr/bin/env python3

import asyncio
import os

import pytest

import uvloop
import uvfiles
from uvfiles import AsyncFile

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


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
async def test_async_write_and_read_roundtrip(tmp_path):
    path = tmp_path / "roundtrip.bin"
    f = await uvfiles.open(str(path), os.O_CREAT | os.O_RDWR | os.O_TRUNC)

    written = await f.write(b"abc\n123")
    assert written == 7
    assert await f.tell() == 7

    assert await f.seek(0) == 0
    content = await f.read()
    assert content == b"abc\n123"
    assert await f.tell() == 7

    await f.close()


@pytest.mark.asyncio
async def test_read_with_size(tmp_path):
    path = tmp_path / "read_size.bin"
    path.write_bytes(b"abcdef")

    f = await uvfiles.open(str(path), os.O_RDONLY)
    assert await f.read(3) == b"abc"
    assert await f.tell() == 3
    assert await f.read(2) == b"de"
    assert await f.tell() == 5
    assert await f.read(10) == b"f"
    assert await f.tell() == 6

    await f.close()


@pytest.mark.asyncio
async def test_seek_tell_whence(tmp_path):
    path = tmp_path / "seek_tell.bin"
    path.write_bytes(b"0123456789")

    f = await uvfiles.open(str(path), os.O_RDONLY)
    assert await f.seek(2) == 2
    assert await f.tell() == 2
    assert await f.seek(3, os.SEEK_CUR) == 5
    assert await f.seek(-1, os.SEEK_END) == 9
    assert await f.tell() == 9

    with pytest.raises(ValueError):
        await f.seek(-11, os.SEEK_END)
    with pytest.raises(ValueError):
        await f.seek(0, 12345)

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
        await f.readable()
    with pytest.raises(ValueError):
        await f.writable()
    with pytest.raises(ValueError):
        await f.seekable()
    with pytest.raises(ValueError):
        await f.isatty()
    with pytest.raises(ValueError):
        await f.seek(0)
    with pytest.raises(ValueError):
        await f.tell()
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


@pytest.mark.asyncio
async def test_async_truncate_default_size_uses_current_pos(tmp_path):
    path = tmp_path / "truncate_default.bin"
    f = await uvfiles.open(str(path), os.O_CREAT | os.O_RDWR | os.O_TRUNC)

    await f.write(b"abcdef")
    await f.seek(3)
    assert await f.truncate() == 3

    await f.seek(0)
    assert await f.read() == b"abc"
    await f.close()


@pytest.mark.asyncio
async def test_async_truncate_explicit_size_and_seek_adjustment(tmp_path):
    path = tmp_path / "truncate_explicit.bin"
    f = await uvfiles.open(str(path), os.O_CREAT | os.O_RDWR | os.O_TRUNC)

    await f.write(b"0123456789")
    await f.seek(8)
    assert await f.truncate(5) == 5
    assert await f.tell() == 5

    await f.seek(0)
    assert await f.read() == b"01234"
    await f.close()


@pytest.mark.asyncio
async def test_async_flush(tmp_path):
    path = tmp_path / "flush.bin"
    f = await uvfiles.open(str(path), os.O_CREAT | os.O_RDWR | os.O_TRUNC)

    await f.write(b"flush-data")
    await f.flush()
    await f.close()

    assert path.read_bytes() == b"flush-data"


@pytest.mark.asyncio
async def test_async_readline_and_eof(tmp_path):
    path = tmp_path / "readline.bin"
    path.write_bytes(b"a\nbc\nlast")

    f = await uvfiles.open(str(path), os.O_RDONLY)
    assert await f.readline() == b"a\n"
    assert await f.readline() == b"bc\n"
    assert await f.readline() == b"last"
    assert await f.readline() == b""
    await f.close()


@pytest.mark.asyncio
async def test_async_readline_size_limit(tmp_path):
    path = tmp_path / "readline_limit.bin"
    path.write_bytes(b"abc\ndef\n")

    f = await uvfiles.open(str(path), os.O_RDONLY)
    assert await f.readline(2) == b"ab"
    assert await f.readline() == b"c\n"
    assert await f.readline() == b"def\n"
    await f.close()


@pytest.mark.asyncio
async def test_async_readlines_with_hint(tmp_path):
    path = tmp_path / "readlines_hint.bin"
    path.write_bytes(b"aa\nbb\ncc\n")

    f = await uvfiles.open(str(path), os.O_RDONLY)
    assert await f.readlines(5) == [b"aa\n", b"bb\n"]
    assert await f.readlines() == [b"cc\n"]
    await f.close()


@pytest.mark.asyncio
async def test_async_writelines_roundtrip(tmp_path):
    path = tmp_path / "writelines.bin"
    f = await uvfiles.open(str(path), os.O_CREAT | os.O_RDWR | os.O_TRUNC)

    await f.writelines([b"line1\n", b"line2\n"])
    await f.seek(0)
    assert await f.read() == b"line1\nline2\n"
    await f.close()


@pytest.mark.asyncio
async def test_async_for_iteration_lines(tmp_path):
    path = tmp_path / "iter_lines.bin"
    path.write_bytes(b"l1\nl2\nl3")

    f = await uvfiles.open(str(path), os.O_RDONLY)
    lines = []
    async for line in f:
        lines.append(line)

    assert lines == [b"l1\n", b"l2\n", b"l3"]
    await f.close()


@pytest.mark.asyncio
async def test_sync_iteration_rejected(tmp_path):
    path = tmp_path / "iter_rejected.bin"
    path.write_bytes(b"x\n")

    f = await uvfiles.open(str(path), os.O_RDONLY)
    with pytest.raises(TypeError):
        iter(f)
    with pytest.raises(TypeError):
        next(f)
    await f.close()


@pytest.mark.asyncio
async def test_new_methods_on_closed_file_raise(tmp_path):
    path = tmp_path / "closed_new_methods.bin"
    path.write_bytes(b"abc\n")

    f = await uvfiles.open(str(path), os.O_RDWR)
    await f.close()

    with pytest.raises(ValueError):
        await f.readline()
    with pytest.raises(ValueError):
        await f.readlines()
    with pytest.raises(ValueError):
        await f.writelines([b"x"])
    with pytest.raises(ValueError):
        await f.truncate()
    with pytest.raises(ValueError):
        await f.flush()
    with pytest.raises(ValueError):
        f.__aiter__()


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
async def test_text_encoding_roundtrip(tmp_path):
    path = tmp_path / "encoding.txt"

    f = await uvfiles.open(str(path), "w", encoding="utf-16-le")
    await f.write("你好")
    await f.close()

    f = await uvfiles.open(str(path), "r", encoding="utf-16-le")
    assert await f.read() == "你好"
    await f.close()


@pytest.mark.asyncio
async def test_text_newline_none_normalizes_reads(tmp_path):
    path = tmp_path / "newline_none.txt"
    path.write_bytes(b"a\r\nb\rc\n")

    f = await uvfiles.open(str(path), "r", newline=None)
    assert await f.readline() == "a\n"
    assert await f.readline() == "b\n"
    assert await f.readline() == "c\n"
    assert await f.readline() == ""
    await f.close()


@pytest.mark.asyncio
async def test_text_newline_empty_preserves_endings(tmp_path):
    path = tmp_path / "newline_empty.txt"
    path.write_bytes(b"a\r\nb\rc\n")

    f = await uvfiles.open(str(path), "r", newline="")
    assert await f.readline() == "a\r\n"
    assert await f.readline() == "b\r"
    assert await f.readline() == "c\n"
    await f.close()


@pytest.mark.asyncio
async def test_async_open_alias(tmp_path):
    path = tmp_path / "alias.txt"
    f = await uvfiles.async_open(str(path), "w+")
    await f.write("alias")
    await f.seek(0)
    assert await f.read() == "alias"
    await f.close()


@pytest.mark.asyncio
async def test_isatty_and_closed_behavior(tmp_path):
    path = tmp_path / "isatty.txt"
    path.write_text("x", encoding="utf-8")

    f = await uvfiles.open(str(path), "r")
    assert await f.isatty() is False
    await f.close()
    with pytest.raises(ValueError):
        await f.isatty()


@pytest.mark.asyncio
async def test_readinto_binary_mode(tmp_path):
    path = tmp_path / "readinto.bin"
    path.write_bytes(b"hello")

    f = await uvfiles.open(str(path), os.O_RDONLY)
    buf = bytearray(8)
    n = await f.readinto(buf)
    assert n == 5
    assert bytes(buf[:n]) == b"hello"
    await f.close()


@pytest.mark.asyncio
async def test_readinto_text_mode_rejected(tmp_path):
    path = tmp_path / "readinto_text.txt"
    path.write_text("hello", encoding="utf-8")

    f = await uvfiles.open(str(path), "r")
    with pytest.raises(TypeError):
        await f.readinto(bytearray(8))
    await f.close()


def test_open_buffering_not_supported(tmp_path):
    path = tmp_path / "buffering.txt"
    path.write_text("hello", encoding="utf-8")

    with pytest.raises(NotImplementedError):
        uvfiles.open(str(path), "r", buffering=1)
