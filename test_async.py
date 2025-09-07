#!/usr/bin/env python3

import asyncio
import os
import uvloop
from uvfiles import open

# Set uvloop policy
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def test_async_open():
    fd = await open("test_async.txt", os.O_RDONLY)
    print(f"File opened with fd: {fd}")


async def main():
    await test_async_open()


if __name__ == "__main__":
    asyncio.run(main())
