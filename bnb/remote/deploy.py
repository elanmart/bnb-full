import logging

import rpyc
from paramiko.client import AutoAddPolicy
from plumbum import ProcessExecutionError
from plumbum.machines.paramiko_machine import ParamikoMachine
from rpyc.core.service import VoidService
from rpyc.core.stream import SocketStream
from rpyc.lib.compat import BYTES_LITERAL

import signal


def timeout(signum, frame):
    raise TimeoutError()


signal.signal(signal.SIGALRM, timeout)
logger = logging.getLogger(__name__)


def _get_machine(host, user, keyfile):
    try:
        for i in range(10):
            logger.debug(f'Connection attempt no: {i}')

            try:
                signal.alarm(15)
                ret = ParamikoMachine(host=host, user=user, keyfile=keyfile,
                                      connect_timeout=15, keep_alive=30, missing_host_policy=AutoAddPolicy())
                signal.alarm(0)

            except TimeoutError:
                pass

            else:
                return ret

        else:
            raise RuntimeError('Connection could not be established')
    finally:
        signal.alarm(0)


class DeployedServer:
    def __init__(self, host, user, keyfile,
                 python_executable='~/anaconda3/bin/python'):

        self.remote_machine = _get_machine(host, user, keyfile)
        self._kill_py()
        self.py = self.remote_machine[python_executable]

        self.proc = self.py.popen(['-m', 'bnb.remote.server'], new_session=True)
        self.local_port = None

        line = ""
        try:
            logger.debug(f'Waiting for the line')

            line = self.proc.stdout.readline()
            self.remote_port = int(line.strip())

        except Exception:
            stdout, stderr = self.proc.communicate()
            self.close()

            raise ProcessExecutionError(
                self.proc.argv, self.proc.returncode, BYTES_LITERAL(line) + stdout, stderr)

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        self.close()

    def _kill_py(self):
        try:
            killall = self.remote_machine['/usr/bin/killall']
            killall.run(['python'])
            killall.run(['python'])

        except Exception as e:
            logger.debug(f'Exception while killing py: {e}')

    def close(self):
        self._kill_py()
        self.remote_machine.close()

    def connect(self, service=VoidService, config=None):
        """Same as :func:`connect <rpyc.utils.factory.connect>`, but with the ``host`` and ``port``
        parameters fixed"""
        config = config or {}

        stream = SocketStream(self.remote_machine.connect_sock(self.remote_port))
        return rpyc.connect_stream(stream, service=service, config=config)
