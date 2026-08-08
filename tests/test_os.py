import errno
import os
import stat as stat_module

import pytest

import uvfiles.os as uvos


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
async def test_os_makedirs_tail_exists_inside_existing_parent(tmp_path):
    # Exercises the recursive branch that swallows FileExistsError when an
    # intermediate directory already exists.
    (tmp_path / "a").mkdir()
    await uvos.makedirs(tmp_path / "a" / "b" / "c")
    assert (tmp_path / "a" / "b" / "c").is_dir()
    await uvos.makedirs(tmp_path / "a" / "b" / "c", exist_ok=True)


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
