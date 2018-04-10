import logging
import signal
import threading
import time
from collections import namedtuple
from multiprocessing import Process

import dill
import rpyc
from rpyc.utils.server import ThreadedServer

from .service import WorkerService
from ..remote.deploy import DeployedServer


class ServiceDead:
    pass


class Worker:

    ConnType = namedtuple('ConnType', ('conn', 'bgsrv'))

    def __init__(self, use_s3):

        self.main     = None  # type: self.ConnType
        self._logger  = None  # type: logging.Logger
        self._use_s3  = use_s3

        self._last_ID       = None
        self._last_entry    = {}
        self._last_update   = None
        self._last_callback = None

        self._should_stop = threading.Event()
        self._tlock       = threading.Lock()

        self._watcher = threading.Thread(target=self._perhaps_restart, daemon=True)
        self._watcher.start()

    def __del__(self):
        self.stop()

    def _forget(self):
        self._last_ID       = None
        self._last_entry    = {}
        self._last_update   = None
        self._last_callback = None

    def _perhaps_restart(self):
        while not self._should_stop.is_set():

            with self._tlock:
                if (self.main is not None) and (self.main.conn._closed):

                    self._logger.error('Closed connection detected!')

                    from bnb import ExecutionManager
                    args, kwargs = ExecutionManager.critical_upadte()

                    if self._last_update is not None:
                        self._last_update(self._last_ID, *args, **kwargs)
                        self._last_callback(ServiceDead())

                    self.stop()
                    self.start()

            time.sleep(10)

    def start(self):
        self._should_stop.clear()

    def stop_main(self):

        if self.main is not None:

            try:
                self.main.conn.close()
                self.main.bgsrv.stop()

            except Exception as e:
                self._logger.error(f"Exception while stopping: {e}")

    @property
    def root(self):
        return self.main.conn.root

    def dispatch(self, callback, upstream_update, db_entry, task):
        """ Dispatches a received task to a (possibly remote) service

        Parameters
        ----------
        callback : Callable
            object called once the execution stops
        upstream_update : Callable
            object that will be used to send progress reports
            from remote service to local Engine
        db_entry : Dict
            database entry associated with this run
        task : bytes
            serialized object to execute
        """

        with self._tlock:

            self._last_ID       = db_entry['ID']
            self._last_entry    = db_entry
            self._last_update   = upstream_update
            self._last_callback = callback

            self._logger.debug(f'Dispatching task: {id(task)}')

            (db_entry,
             task) = (dill.dumps(db_entry),
                      dill.dumps(task))

            self.root.exposed_dispatch(callback=callback, upstream_update=upstream_update,
                                       db_entry=db_entry, task=task, start_s3=self._use_s3)

            self._logger.debug('Worker dispatch done')

    def stop(self):

        self._logger.debug('Stopping')

        self._should_stop.set()
        self._forget()

        self.stop_main()


class LocalWorker(Worker):

    def __init__(self, port, patience=3):
        super().__init__(False)

        self._logger = logging.getLogger(f'{self.__class__.__name__}-{port}')
        self._logger.debug(f'Starting local worker on port {port}')

        self.port     = port
        self.patience = patience

        self._server  = None

        self._logger.info('Worker ready')

    def start(self):

        self._server = self._start_server(port=self.port)
        self.main    = self._connect(self.port)

        self._logger.debug(f'Local worker should be available at port {self.port}')

        return self

    def stop(self):
        super().stop()

        if self._server is not None:
            self._server.terminate()
            self._server.join()

    def _start_server(self, port):

        self._logger.debug(f'Attempting to start on port {port}')

        def _start_server(port):

            def _raise(*_, **__):
                raise KeyboardInterrupt

            # signal.signal(signal.SIGTERM, _raise)  # TODO(elan)

            server = ThreadedServer(service=WorkerService, port=port)
            server.start()

        p = threading.Thread(target=_start_server, args=(port,))  # TODO(elan)
        p.daemon = True
        p.start()

        self._logger.debug(f'Should be running on port {port}, Process.is_alive(): {p.is_alive()}')

        return p

    def _connect(self, port):

        self._logger.debug(f'Trying to connect on port {port}')

        for i in range(self.patience):
            try:
                self._logger.debug(f'Attempt no: {i}')
                
                conn  = rpyc.connect(host="localhost", port=port, service=rpyc.VoidService)
                bgsrv = rpyc.BgServingThread(conn)

                conn.ping()

                return self.ConnType(conn, bgsrv)

            except ConnectionRefusedError:
                time.sleep(2)

        raise ConnectionRefusedError


class SSHWorker(Worker):

    def __init__(self, host, user, keyfile):
        super().__init__(True)

        self._logger = logging.getLogger(f'{self.__class__.__name__}-{host}')
        self._logger.debug(f'Starting aws worker for {user}@{host}')

        self.host = host
        self.user = user
        self.key  = keyfile

        self._server = None  # type: DeployedServer

    def start(self):
        super().start()

        self._server = DeployedServer(self.host, self.user, self.key)
        self._logger.debug(f'Server deployed for {self.user}@{self.host}')

        self.main = self._connect(self._server)

        self._logger.info(f'SSH worker {self} should be available')

        return self

    def stop(self):
        super().stop()

        self._server.close()

    def _connect(self, server):
        conn  = server.connect(service=rpyc.VoidService)
        bgsrv = rpyc.BgServingThread(conn)

        conn.ping(timeout=15)

        return self.ConnType(conn, bgsrv)
