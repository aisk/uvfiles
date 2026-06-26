# uvfiles

`uvfiles` is an asynchronous file I/O library built on top of `libuv`, with an API designed to feel close to `aiofiles`.
Its goal is to provide `libuv`'s portability and, on Linux, leverage `io_uring` (when available) for truly asynchronous file access.

## Requirements

- Python 3.10+
- [`uvloop`](https://github.com/MagicStack/uvloop) (a hard runtime dependency)

Because `uvfiles` runs on `uvloop`, it supports the same platforms as `uvloop`: **Linux and macOS**. Windows is not supported. On Linux, file operations transparently use `libuv`'s `io_uring` backend when the kernel provides it.

`uvfiles` only works with a `uvloop` event loop; using it on the stdlib asyncio loop raises a clear `RuntimeError`.

## Installation

```bash
uv add uvfiles    # or: pip install uvfiles
```

For local development in the project directory:

```bash
uv sync
```

## Quick Start

```python
import asyncio
import uvloop
from uvfiles import async_open

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def main() -> None:
    async with await async_open("example.txt", "w") as f:
        await f.write("hello uvfiles\n")

    async with await async_open("example.txt", "r") as f:
        print(await f.read())


asyncio.run(main())
```

## API

The public API mirrors `aiofiles` and Python's builtin `open()`.

### `uvfiles.open` / `uvfiles.async_open`

Two names for the same function. It returns an awaitable that resolves to an `AsyncFile`. It accepts either a builtin-style mode string (`"r"`, `"w+b"`, `"a"`, `"x"`, ...) or an integer `os.O_*` flag bitmask, plus `encoding` / `errors` / `newline` for text mode.

`AsyncFile` is a file-object-shaped wrapper whose methods are coroutines: `read`, `readall`, `read1`, `readline`, `readlines`, `readinto`, `write`, `writelines`, `seek`, `tell`, `truncate`, `flush`, `close`, `readable`, `writable`, `seekable`, `isatty`, plus `async with` and `async for`. Operations on a single file object are serialized with a lock, so concurrent coroutines cannot corrupt its position.

### `uvfiles.os`

Async equivalents of `os` / `aiofiles.os` filesystem helpers:
`stat`, `lstat`, `remove` / `unlink`, `rename`, `replace`, `renames`, `mkdir`, `makedirs`, `rmdir`, `removedirs`, `link`, `symlink`, `readlink`, `access`, `listdir`, `scandir`, `sendfile`, `statvfs`, `getcwd`.

### `uvfiles.os.path`

Async equivalents of `os.path` / `aiofiles.os.path`:
`exists`, `isfile`, `isdir`, `islink`, `getsize`, `getmtime`, `getatime`, `getctime`, `samefile`, `sameopenfile`, `abspath`, `ismount`.

### `uvfiles.tempfile`

Async equivalents of `aiofiles.tempfile`:
`TemporaryFile`, `NamedTemporaryFile`, `SpooledTemporaryFile`, `TemporaryDirectory`.

```python
import uvfiles.os
import uvfiles.tempfile

async def demo():
    async with uvfiles.tempfile.NamedTemporaryFile() as f:
        await f.write(b"data")

    for entry in await uvfiles.os.scandir("."):
        if entry.is_file():
            print(entry.name, await uvfiles.os.path.getsize(entry.path))
```

## Development

```bash
uv sync                 # install dev environment
uv run pytest -v        # run tests
uv run pyrefly check    # type check
```

## License

MIT
