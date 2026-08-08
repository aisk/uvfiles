import os

import pytest

import uvfiles.tempfile as uvtemp
from uvfiles import AsyncFile


@pytest.mark.asyncio
async def test_tempfile_temporaryfile_roundtrip():
    async with uvtemp.TemporaryFile() as f:
        assert isinstance(f, AsyncFile)
        await f.write(b"temp-data")
        await f.seek(0)
        assert await f.read() == b"temp-data"
    assert f.closed is True


@pytest.mark.asyncio
async def test_tempfile_temporaryfile_text_mode():
    async with uvtemp.TemporaryFile(mode="w+", encoding="utf-8") as f:
        await f.write("你好")
        await f.seek(0)
        assert await f.read() == "你好"


@pytest.mark.asyncio
async def test_tempfile_named_visible_and_deleted():
    async with uvtemp.NamedTemporaryFile() as f:
        name = f.name
        assert os.path.exists(name)
        await f.write(b"named")
        await f.seek(0)
        assert await f.read() == b"named"
    # delete=True removes the file on close.
    assert not os.path.exists(name)


@pytest.mark.asyncio
async def test_tempfile_named_delete_false_kept():
    async with uvtemp.NamedTemporaryFile(delete=False) as f:
        name = f.name
        await f.write(b"keep")
    try:
        assert os.path.exists(name)
        with open(name, "rb") as raw:
            assert raw.read() == b"keep"
    finally:
        os.unlink(name)


@pytest.mark.asyncio
async def test_tempfile_await_form():
    f = await uvtemp.TemporaryFile()
    try:
        await f.write(b"x")
        await f.seek(0)
        assert await f.read() == b"x"
    finally:
        await f.close()
    assert f.closed is True


@pytest.mark.asyncio
async def test_tempfile_temporary_directory():
    async with uvtemp.TemporaryDirectory() as d:
        assert os.path.isdir(d)
        child = os.path.join(d, "f.txt")
        async with uvtemp.NamedTemporaryFile(dir=d) as f:
            assert os.path.dirname(f.name) == d
        with open(child, "w", encoding="utf-8") as raw:
            raw.write("x")
    # Directory and its contents are removed on exit.
    assert not os.path.exists(d)


@pytest.mark.asyncio
async def test_tempfile_spooled_rollover():
    async with uvtemp.SpooledTemporaryFile(max_size=4) as f:
        await f.write(b"ab")
        await f.write(b"cdef")  # exceeds max_size, rolls over to disk
        await f.rollover()
        await f.seek(0)
        assert await f.read() == b"abcdef"
    assert f.closed is True
