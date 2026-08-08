#!/usr/bin/env python3

import asyncio
import errno
import os
import stat as stat_module

import pytest

import uvloop
import uvfiles
import uvfiles.os as uvos
import uvfiles.tempfile as uvtemp
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


def test_open_buffering_not_supported(tmp_path):
    path = tmp_path / "buffering.txt"
    path.write_text("hello", encoding="utf-8")

    with pytest.raises(NotImplementedError):
        uvfiles.open(str(path), "r", buffering=1)


@pytest.mark.asyncio
async def test_concurrent_reads_do_not_corrupt_position(tmp_path):
    path = tmp_path / "concurrent_read.bin"
    payload = bytes(range(256)) * 64  # 16 KiB, larger than a single uv read chunk
    path.write_bytes(payload)

    f = await uvfiles.open(str(path), os.O_RDONLY)
    try:
        # Two full reads issued concurrently on the same file object. The lock
        # serializes them, so each must observe a consistent cursor: one returns
        # the whole file, the other (running after) sees EOF.
        first, second = await asyncio.gather(f.read(), f.read())
    finally:
        await f.close()

    assert {len(first), len(second)} == {len(payload), 0}
    full = first if first else second
    assert full == payload


@pytest.mark.asyncio
async def test_concurrent_writes_are_serialized(tmp_path):
    path = tmp_path / "concurrent_write.bin"
    f = await uvfiles.open(str(path), os.O_CREAT | os.O_RDWR | os.O_TRUNC)
    try:
        chunk_a = b"a" * 4096
        chunk_b = b"b" * 4096
        await asyncio.gather(f.write(chunk_a), f.write(chunk_b))
        await f.seek(0)
        content = await f.read()
    finally:
        await f.close()

    # Serialized writes never interleave, so the file is one chunk followed by
    # the other with no overlap or lost bytes.
    assert content in (chunk_a + chunk_b, chunk_b + chunk_a)


@pytest.mark.asyncio
async def test_os_remove(tmp_path):
    path = tmp_path / "remove_me.txt"
    path.write_text("x", encoding="utf-8")

    await uvos.remove(path)
    assert not path.exists()


@pytest.mark.asyncio
async def test_os_remove_missing_raises(tmp_path):
    with pytest.raises(OSError):
        await uvos.remove(tmp_path / "does_not_exist")


@pytest.mark.asyncio
async def test_os_unlink_is_remove_alias(tmp_path):
    path = tmp_path / "unlink_me.txt"
    path.write_text("x", encoding="utf-8")

    assert uvos.unlink is uvos.remove
    await uvos.unlink(path)
    assert not path.exists()


@pytest.mark.asyncio
async def test_os_rename(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    src.write_text("data", encoding="utf-8")

    await uvos.rename(src, dst)
    assert not src.exists()
    assert dst.read_text(encoding="utf-8") == "data"


@pytest.mark.asyncio
async def test_os_mkdir_and_rmdir(tmp_path):
    d = tmp_path / "sub"

    await uvos.mkdir(d)
    assert d.is_dir()

    await uvos.rmdir(d)
    assert not d.exists()


@pytest.mark.asyncio
async def test_os_mkdir_existing_raises(tmp_path):
    d = tmp_path / "exists"
    d.mkdir()

    with pytest.raises(OSError):
        await uvos.mkdir(d)


@pytest.mark.asyncio
async def test_ospath_exists(tmp_path):
    f = tmp_path / "f.txt"
    f.write_text("x", encoding="utf-8")

    assert await uvos.path.exists(f) is True
    assert await uvos.path.exists(tmp_path / "nope") is False


@pytest.mark.asyncio
async def test_ospath_isfile_isdir(tmp_path):
    f = tmp_path / "f.txt"
    f.write_text("x", encoding="utf-8")
    d = tmp_path / "d"
    d.mkdir()

    assert await uvos.path.isfile(f) is True
    assert await uvos.path.isfile(d) is False
    assert await uvos.path.isfile(tmp_path / "nope") is False

    assert await uvos.path.isdir(d) is True
    assert await uvos.path.isdir(f) is False


@pytest.mark.asyncio
async def test_ospath_islink_and_exists_follow(tmp_path):
    target = tmp_path / "target.txt"
    target.write_text("x", encoding="utf-8")
    link = tmp_path / "link.txt"
    link.symlink_to(target)

    assert await uvos.path.islink(link) is True
    assert await uvos.path.islink(target) is False
    # exists() follows the symlink to a real file.
    assert await uvos.path.exists(link) is True

    target.unlink()
    # Broken symlink: islink stays True, exists is False.
    assert await uvos.path.islink(link) is True
    assert await uvos.path.exists(link) is False


@pytest.mark.asyncio
async def test_ospath_getsize_and_times(tmp_path):
    f = tmp_path / "f.txt"
    f.write_bytes(b"hello")

    assert await uvos.path.getsize(f) == 5 == os.path.getsize(f)
    assert await uvos.path.getmtime(f) == pytest.approx(os.path.getmtime(f))
    assert await uvos.path.getatime(f) == pytest.approx(os.path.getatime(f))
    assert await uvos.path.getctime(f) == pytest.approx(os.path.getctime(f))


@pytest.mark.asyncio
async def test_ospath_getsize_missing_raises(tmp_path):
    with pytest.raises(OSError):
        await uvos.path.getsize(tmp_path / "nope")


@pytest.mark.asyncio
async def test_ospath_samefile(tmp_path):
    f = tmp_path / "f.txt"
    f.write_text("x", encoding="utf-8")
    link = tmp_path / "link.txt"
    link.symlink_to(f)
    other = tmp_path / "other.txt"
    other.write_text("y", encoding="utf-8")

    assert await uvos.path.samefile(f, link) is True
    assert await uvos.path.samefile(f, other) is False


@pytest.mark.asyncio
async def test_ospath_abspath():
    assert await uvos.path.abspath("foo") == os.path.abspath("foo")


@pytest.mark.asyncio
async def test_os_access(tmp_path):
    f = tmp_path / "f.txt"
    f.write_text("x", encoding="utf-8")

    assert await uvos.access(f, os.F_OK) is True
    assert await uvos.access(f, os.R_OK) is True
    assert await uvos.access(tmp_path / "nope", os.F_OK) is False


@pytest.mark.asyncio
async def test_os_link(tmp_path):
    src = tmp_path / "src.txt"
    src.write_text("data", encoding="utf-8")
    dst = tmp_path / "hardlink.txt"

    await uvos.link(src, dst)
    assert dst.read_text(encoding="utf-8") == "data"
    assert src.stat().st_ino == dst.stat().st_ino


@pytest.mark.asyncio
async def test_os_symlink_and_readlink(tmp_path):
    target = tmp_path / "target.txt"
    target.write_text("data", encoding="utf-8")
    link = tmp_path / "link.txt"

    await uvos.symlink(target, link)
    assert link.is_symlink()
    assert link.read_text(encoding="utf-8") == "data"

    assert await uvos.readlink(link) == str(target)


@pytest.mark.asyncio
async def test_os_replace_overwrites(tmp_path):
    src = tmp_path / "src.txt"
    src.write_text("new", encoding="utf-8")
    dst = tmp_path / "dst.txt"
    dst.write_text("old", encoding="utf-8")

    await uvos.replace(src, dst)
    assert not src.exists()
    assert dst.read_text(encoding="utf-8") == "new"


@pytest.mark.asyncio
async def test_os_makedirs_and_removedirs(tmp_path):
    nested = tmp_path / "a" / "b" / "c"

    await uvos.makedirs(nested)
    assert nested.is_dir()

    # exist_ok behavior
    with pytest.raises(OSError):
        await uvos.makedirs(nested)
    await uvos.makedirs(nested, exist_ok=True)

    await uvos.removedirs(nested)
    assert not (tmp_path / "a").exists()


@pytest.mark.asyncio
async def test_os_renames(tmp_path):
    src = tmp_path / "old" / "file.txt"
    src.parent.mkdir()
    src.write_text("data", encoding="utf-8")
    dst = tmp_path / "new" / "sub" / "file.txt"

    await uvos.renames(src, dst)
    assert dst.read_text(encoding="utf-8") == "data"
    # The now-empty source tree is pruned.
    assert not (tmp_path / "old").exists()


@pytest.mark.asyncio
async def test_os_getcwd():
    assert await uvos.getcwd() == os.getcwd()


@pytest.mark.asyncio
async def test_os_sendfile(tmp_path):
    src = tmp_path / "src.bin"
    payload = bytes(range(256)) * 16  # 4 KiB
    src.write_bytes(payload)
    dst = tmp_path / "dst.bin"

    in_fd = os.open(src, os.O_RDONLY)
    out_fd = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        total = 0
        while total < len(payload):
            sent = await uvos.sendfile(out_fd, in_fd, total, len(payload) - total)
            if sent == 0:
                break
            total += sent
    finally:
        os.close(in_fd)
        os.close(out_fd)

    assert total == len(payload)
    assert dst.read_bytes() == payload


@pytest.mark.asyncio
async def test_os_statvfs(tmp_path):
    st = await uvos.statvfs(tmp_path)

    # libuv's uv_fs_statfs and the platform's os.statvfs disagree on the meaning
    # of some fields (e.g. f_bsize is the fundamental block size for libuv but the
    # preferred I/O size on macOS), so sanity-check the values instead of
    # comparing against os.statvfs.
    assert isinstance(st, os.statvfs_result)
    assert st.f_bsize > 0
    assert st.f_blocks > 0
    assert st.f_bfree >= 0
    assert st.f_bavail <= st.f_blocks
    assert st.f_files > 0


@pytest.mark.asyncio
async def test_os_statvfs_missing_raises(tmp_path):
    with pytest.raises(OSError):
        await uvos.statvfs(tmp_path / "nope")


@pytest.mark.asyncio
async def test_readall(tmp_path):
    path = tmp_path / "readall.bin"
    path.write_bytes(b"0123456789")

    f = await uvfiles.open(str(path), os.O_RDONLY)
    try:
        assert await f.read(3) == b"012"
        assert await f.readall() == b"3456789"
    finally:
        await f.close()


@pytest.mark.asyncio
async def test_read1_single_chunk(tmp_path):
    path = tmp_path / "read1.bin"
    path.write_bytes(b"abcdef")

    f = await uvfiles.open(str(path), os.O_RDONLY)
    try:
        chunk = await f.read1(3)
        assert chunk == b"abc"
        assert await f.tell() == 3
    finally:
        await f.close()


@pytest.mark.asyncio
async def test_read1_text_mode_rejected(tmp_path):
    path = tmp_path / "read1_text.txt"
    path.write_text("hello", encoding="utf-8")

    f = await uvfiles.open(str(path), "r")
    try:
        with pytest.raises(TypeError):
            await f.read1(3)
    finally:
        await f.close()


@pytest.mark.asyncio
async def test_os_stat_matches_stdlib(tmp_path):
    path = tmp_path / "stat_me.txt"
    path.write_bytes(b"hello world")

    st = await uvos.stat(path)
    expected = os.stat(path)

    assert isinstance(st, os.stat_result)
    assert st.st_size == 11 == expected.st_size
    assert st.st_mode == expected.st_mode
    assert st.st_ino == expected.st_ino
    assert st.st_mtime_ns == expected.st_mtime_ns


@pytest.mark.asyncio
async def test_os_stat_missing_raises(tmp_path):
    with pytest.raises(OSError):
        await uvos.stat(tmp_path / "nope")


@pytest.mark.asyncio
async def test_os_listdir(tmp_path):
    (tmp_path / "a.txt").write_text("x", encoding="utf-8")
    (tmp_path / "b.txt").write_text("y", encoding="utf-8")
    (tmp_path / "sub").mkdir()

    names = await uvos.listdir(tmp_path)
    assert sorted(names) == ["a.txt", "b.txt", "sub"]
    assert sorted(names) == sorted(os.listdir(tmp_path))


@pytest.mark.asyncio
async def test_os_listdir_empty(tmp_path):
    d = tmp_path / "empty"
    d.mkdir()
    assert await uvos.listdir(d) == []


@pytest.mark.asyncio
async def test_os_listdir_missing_raises(tmp_path):
    with pytest.raises(OSError):
        await uvos.listdir(tmp_path / "nope")


@pytest.mark.asyncio
async def test_os_scandir_entries(tmp_path):
    f = tmp_path / "file.txt"
    f.write_text("hello", encoding="utf-8")
    d = tmp_path / "dir"
    d.mkdir()

    entries = await uvos.scandir(tmp_path)
    by_name = {e.name: e for e in entries}
    assert set(by_name) == {"file.txt", "dir"}

    file_entry = by_name["file.txt"]
    assert file_entry.is_file() is True
    assert file_entry.is_dir() is False
    assert file_entry.is_symlink() is False
    assert file_entry.path == str(f)
    assert os.fspath(file_entry) == str(f)
    assert file_entry.stat().st_size == 5

    dir_entry = by_name["dir"]
    assert dir_entry.is_dir() is True
    assert dir_entry.is_file() is False


@pytest.mark.asyncio
async def test_os_scandir_symlink(tmp_path):
    target = tmp_path / "target_dir"
    target.mkdir()
    link = tmp_path / "link"
    link.symlink_to(target)

    entries = {e.name: e for e in await uvos.scandir(tmp_path)}
    link_entry = entries["link"]

    assert link_entry.is_symlink() is True
    # follow_symlinks=True resolves to the directory; =False sees the link itself.
    assert link_entry.is_dir() is True
    assert link_entry.is_dir(follow_symlinks=False) is False


@pytest.mark.asyncio
async def test_os_scandir_context_manager(tmp_path):
    (tmp_path / "x.txt").write_text("x", encoding="utf-8")

    with await uvos.scandir(tmp_path) as entries:
        assert [e.name for e in entries] == ["x.txt"]


@pytest.mark.asyncio
async def test_os_lstat_does_not_follow_symlink(tmp_path):
    target = tmp_path / "target.txt"
    target.write_bytes(b"data")
    link = tmp_path / "link.txt"
    link.symlink_to(target)

    link_stat = await uvos.lstat(link)
    target_stat = await uvos.stat(link)

    assert stat_module.S_ISLNK(link_stat.st_mode)
    assert not stat_module.S_ISLNK(target_stat.st_mode)
    assert target_stat.st_size == 4


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


@pytest.mark.asyncio
async def test_os_errors_map_to_oserror_subclasses(tmp_path):
    missing = tmp_path / "missing"

    with pytest.raises(FileNotFoundError) as excinfo:
        await uvos.remove(missing)
    assert excinfo.value.errno == errno.ENOENT
    assert excinfo.value.filename == str(missing)

    with pytest.raises(FileNotFoundError):
        await uvos.stat(missing)

    existing = tmp_path / "dir"
    existing.mkdir()
    with pytest.raises(FileExistsError):
        await uvos.mkdir(existing)

    with pytest.raises(FileNotFoundError) as excinfo:
        await uvos.rename(missing, tmp_path / "dst")
    assert excinfo.value.filename == str(missing)
    assert excinfo.value.filename2 == str(tmp_path / "dst")


@pytest.mark.asyncio
async def test_os_makedirs_tail_exists_inside_existing_parent(tmp_path):
    # Exercises the recursive branch that swallows FileExistsError when an
    # intermediate directory already exists.
    (tmp_path / "a").mkdir()
    await uvos.makedirs(tmp_path / "a" / "b" / "c")
    assert (tmp_path / "a" / "b" / "c").is_dir()
    await uvos.makedirs(tmp_path / "a" / "b" / "c", exist_ok=True)


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
