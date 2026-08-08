import os

import pytest

import uvfiles.os as uvos


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
