#!/usr/bin/env python3

import asyncio
import io
import os

import pytest

import uvloop
import uvfiles

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


@pytest.mark.asyncio
async def test_async_open():
    """Test async file opening functionality"""
    # Test opening existing file
    f = await uvfiles.open(__file__, os.O_RDONLY)
    assert isinstance(f, io.IOBase)
    assert f.fileno() > 2  # Should not be stdin/stdout/stderr

    # Verify we can read from the file
    content = f.read()
    assert len(content) > 0
    f.close()
