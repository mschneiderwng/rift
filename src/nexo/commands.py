import re
import subprocess
from subprocess import PIPE, Popen
from typing import Sequence

import structlog


class NoSuchDatasetError(subprocess.CalledProcessError):
    def __init__(self, returncode, cmd, output=None, stderr=None):
        super().__init__(returncode, cmd, output, stderr)


class DestinationFilesystemExists(subprocess.CalledProcessError):
    def __init__(self, returncode, cmd, output=None, stderr=None):
        super().__init__(returncode, cmd, output, stderr)


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
                    raise NoSuchDatasetError(proc.returncode, proc.args, stdout, stderr)
                elif re.match(".* destination '.*' exists", str(stderr)):
                    raise DestinationFilesystemExists(proc.returncode, proc.args, stdout, stderr)
                else:
                    raise subprocess.CalledProcessError(proc.returncode, proc.args, stdout, stderr)

        return outputs[-1][0].decode().strip()
