import pytest

import uvfiles


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
async def test_text_readline_spanning_chunks(tmp_path):
    # A line longer than the 8 KiB read chunk must be assembled across reads.
    path = tmp_path / "long_line.txt"
    long_line = "x" * 20000
    path.write_text(long_line + "\n" + "tail\n", encoding="utf-8")

    f = await uvfiles.open(str(path), "r")
    assert await f.readline() == long_line + "\n"
    assert await f.readline() == "tail\n"
    assert await f.readline() == ""
    await f.close()


@pytest.mark.asyncio
async def test_text_readline_size_limit(tmp_path):
    path = tmp_path / "limit.txt"
    path.write_text("abcdef\nxyz\n", encoding="utf-8")

    f = await uvfiles.open(str(path), "r")
    assert await f.readline(3) == "abc"
    assert await f.readline(3) == "def"
    assert await f.readline(3) == "\n"
    assert await f.readline() == "xyz\n"
    await f.close()
