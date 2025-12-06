import asyncio
import re
from asyncio import create_subprocess_exec
from asyncio.subprocess import PIPE
from subprocess import PIPE
from typing import Sequence

import structlog


class SubprocessError(Exception):
    def __init__(self, message, cmd):
        super().__init__(message)
        self.message = message
        self.cmd = cmd

    def __str__(self):
        return f"Command `{' '.join(self.cmd)}` failed: {self.message}"


class NoSuchDatasetError(SubprocessError):
    def __init__(self, message, cmd):
        super().__init__(message, cmd)


class DestinationFilesystemExists(SubprocessError):
    def __init__(self, message, cmd):
        super().__init__(message, cmd)


class Runner:
    def run(self, command: Sequence[str], *others: Sequence[str]) -> str:
        """
        Run shell commands. If more than one command is provided, the commands will be piped and the output of the last command returned.
        """
        raise NotImplementedError


class SystemRunner(Runner):
    def run(self, command: Sequence[str], *others: Sequence[str]) -> str:
        """
        Run shell commands. If more than one command is provided, the commands will be piped and the output of the last command returned.
        """
        return asyncio.run(self.main(command, *others))

    async def main(self, command: Sequence[str], *others: Sequence[str]) -> str:
        """
        Run shell commands. If more than one command is provided, the commands will be piped and the output of the last command returned.
        """
        output = []  # stdout of last command
        errors = []  # list of all exceptions of all processes

        async def forward(src, dest):
            """Read data from src and write it to dest."""
            while True:
                chunk = await src.read(4096)
                if not chunk:
                    dest.write_eof()
                    await dest.drain()
                    break
                dest.write(chunk)
                await dest.drain()

        async def stderr_watch(process, cmd):
            """Monitor stderr and raise exceptions if anything read"""
            err = await process.stderr.read()
            if err.strip():
                raise SubprocessError(err.decode(), cmd)

        async def stream_reader(stream):
            """Read the output of a stream to the output buffer"""
            while True:
                line = await stream.readline()
                if not line:
                    break
                output.append(line.decode())

        commands = [command] + list(others)
        log = structlog.get_logger()
        log.debug("> " + " | ".join(map(" ".join, commands)))

        # create processes to run commands
        processes = [await create_subprocess_exec(*commands[0], stdout=PIPE, stderr=PIPE)]
        for cmd in commands[1:]:
            processes.append(await create_subprocess_exec(*cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE))

        try:
            async with asyncio.TaskGroup() as tg:
                # watch for errors
                for process, cmd in zip(processes, commands):
                    tg.create_task(stderr_watch(process, cmd))

                # forward stdout -> stdin
                for prev, next in zip(processes[:-1], processes[1:]):
                    tg.create_task(forward(prev.stdout, next.stdin))

                # read stdout from last stream
                tg.create_task(stream_reader(processes[-1].stdout))

        # check for errors
        except* Exception as eg:
            # terminate processes if there was an error in one of them
            for p in processes:
                try:
                    p.terminate()
                except Exception:
                    pass
            # save exceptions to raise outside TaskGroup
            errors = eg.exceptions

        # wait for all processes to finish
        for p in processes:
            await p.communicate()

        # raise if there was an exception
        for e in errors:
            if isinstance(e, SubprocessError):
                if "dataset does not exist" in str(e):
                    raise NoSuchDatasetError(str(e), e.cmd)
                if re.match(".* destination '.*' exists", str(e)):
                    raise DestinationFilesystemExists(str(e), e.cmd)
            raise e

        return "".join(output).strip()
