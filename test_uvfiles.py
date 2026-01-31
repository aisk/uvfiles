#!/usr/bin/env python3

import asyncio
import os

import pytest

import uvloop
import uvfiles
from uvfiles import AsyncFile

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


@pytest.mark.asyncio
async def test_async_open():
    """Test async file opening functionality"""
    # Test opening existing file
    f = await uvfiles.open(__file__, os.O_RDONLY)
    assert isinstance(f, AsyncFile)
    assert f.fileno() > 2  # Should not be stdin/stdout/stderr
    assert f.name == __file__
    assert f.mode == "r"
    assert f.closed is False
    assert f.readable() is True
    assert f.writable() is False

    # Verify read() raises NotImplementedError for now
    with pytest.raises(NotImplementedError):
        f.read()

    # close() also raises NotImplementedError for now
    with pytest.raises(NotImplementedError):
        f.close()
