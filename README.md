# uvfiles

`uvfiles` is an asynchronous file I/O library built on top of `libuv`, with an API designed to feel close to `aiofiles`.
Its goal is to provide `libuv`'s cross-platform portability and, on Linux, leverage `io_uring` (when available) for truly asynchronous file access.

## Installation

Install dependencies with `uv` in the project directory:

```bash
uv sync
```

## Simple Example

`uvfiles` requires a `uvloop` event loop.

```python
import asyncio
import uvloop
from uvfiles import async_open

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def main() -> None:
    async_file = await async_open("example.txt", "w")
    await async_file.write("hello uvfiles\n")
    await async_file.close()

    async_file = await async_open("example.txt", "r")
    content = await async_file.read()
    await async_file.close()

    print(content)


asyncio.run(main())
```
