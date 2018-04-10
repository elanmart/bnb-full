import logging
import os
import signal
import sys
import threading
import time

import dill
import rpyc
from rpyc import AsyncResultTimeout
from multiprocessing import Process

from bnb.defaults import prepare
from . import s3
from ..track import execution
from ..utils import root_logger


class WorkerService(rpyc.VoidService):

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        _ = root_logger.get_root_logger()

        self._name = f'{self.__class__.__name__}-{str(id(self))[:7]}'
        self._stop = threading.Event()
        self._run  = None

        self._ping_thread = None
        self._background  = []

        self._logger  = logging.getLogger(self._name)
        self._logger.debug(f'WorkerService {id(self)} started')

    def __del__(self):

        self._stop.set()
        for r in self._background:
            r.join()

        self._logger.debug('Service destroyed')

    def on_connect(self):
        self._ping_thread = threading.Thread(target=self._start_ping,
                                             args=(self._conn, ),
                                             daemon=True)
        self._ping_thread.start()

        self._logger.debug(f'New connection: {self._conn}')

    def _start_ping(self, conn):
        self._logger.debug('Ping thread started')

        while True:
            try:
                conn.ping()

            except AsyncResultTimeout as e:
                self._logger.warning(f'AsyncResultTimeout: {e}')

            except EOFError:
                self._logger.debug('Ping <EOF>ed')
                break

            except Exception as e:
                self._logger.error(f'Uncaught exception: {e}')
                raise

            else:
                self._logger.debug('Ping OK')
                time.sleep(30)

    def _get_name(self, db_entry):
        return db_entry['rich_id']['name']

    def _unpickle(self, db_entry, task):
        db_entry = dill.loads(db_entry)
        name = self._get_name(db_entry)

        if name in os.listdir(os.getcwd()):
            sys.path.append(os.path.join(os.getcwd(), name))

        task = dill.loads(task)

        return db_entry, task

    def _dispatch(self, callback, upstream_update,
                  db_entry, f, args, kwargs):

        self._logger.debug(f'Service dispatching: {(f, args, kwargs)}')

        ctx = execution.ExecutionContext(db_entry=db_entry, upstream_update=upstream_update)
        ret = 'NA'

        try:
            # pid = os.getpid()
            # os.system(f'sudo renice -n -19 -p {pid}')
            ret = ctx.run(f, args, kwargs)

        except Exception as e:
            ret = e
            self._logger.error(f'Uncaught exception from ExecutionContext.run(...): {e}')

        finally:
            self._logger.debug('Stopping running background jobs')

            self._stop.set()
            for t in self._background:
                t.join()

            self._logger.debug('Service dispatch is now done, invoke callback')

            self._run = None
            callback(ret)

    def exposed_dispatch(self, callback, upstream_update,
                         db_entry, task,
                         start_s3=False):

        assert self._run is None, "Dispatch called multiple times"

        self._logger.debug(f'Service workdir: {os.getcwd()}')
        self._stop.clear()

        db_entry, task = self._unpickle(db_entry, task)

        (ID,
         name) = (db_entry['ID'],
                  db_entry['rich_id']['name'])

        (f,
         args, 
         kwargs) = task

        prepare(ID=ID, name=name)

        if start_s3:
            self.exposed_start_sync_worker(db_entry=db_entry)

        self._run = threading.Thread(target=self._dispatch,
                                     args=(callback, upstream_update,
                                           db_entry, f, args, kwargs),
                                     daemon=True)
        self._run.start()

    def exposed_start_sync_worker(self, db_entry, interval=30, exclude=None):
        self._logger.debug(f's3 sync start requested. Starting for entry: {id(db_entry)}')

        (src,
         dst) = s3.get_s3_info(db_entry)

        if src is None or dst is None:
            self._logger.debug('src or dst was None, not syncing to s3')
            return

        def _work(self):

            while True:
                s3.safe_s3_sync(src, dst, exclude=exclude)
                if self._stop.is_set():
                    self._logger.debug('Im done sycing, break')
                    break

                time.sleep(interval)

        t = threading.Thread(target=_work, args=(self, ), daemon=True)

        self._background.append(t)
        t.start()
