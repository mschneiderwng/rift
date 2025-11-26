import re
from subprocess import PIPE, Popen
from typing import Sequence

import structlog


class SubprocessError(Exception):
    def __init__(self, message, cmd):
        super().__init__(message)
        self.message = message
        self.cmd = " ".join(cmd)

    def __str__(self):
        return f"Command `{self.cmd}` failed: {self.message}"


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
        commands = [command] + list(others)

        log = structlog.get_logger()
        log.debug("> " + " | ".join(map(" ".join, commands)))

        processes = [Popen(commands[0], stdout=PIPE, stderr=PIPE)]
        for cmd in commands[1:]:
            processes.append(Popen(cmd, stdin=processes[-1].stdout, stdout=PIPE, stderr=PIPE))

        # Interact with process: Send data to stdin and close it
        outputs = list(reversed([proc.communicate() for proc in reversed(processes)]))

        # check for errors
        for (stdout, stderr), proc in zip(outputs, processes):
            if proc.returncode != 0:
                if "dataset does not exist" in str(stderr):
                    raise NoSuchDatasetError(str(stderr), proc.args)
                elif re.match(".* destination '.*' exists", str(stderr)):
                    raise DestinationFilesystemExists(str(stderr), proc.args)
                else:
                    raise SubprocessError(str(stderr), proc.args)

        return outputs[-1][0].decode().strip()
