import logging
from functools import partial
from queue import Queue

from bnb.dispatch.s3 import get_s3_info, safe_s3_sync
from bnb.dispatch.workers import LocalWorker, SSHWorker


class WorkerManager:

    def __init__(self, n_local=1, local_port=11111,
                 remote_hosts=None, user=None, keyfile=None):

        self.n_local      = n_local
        self.local_port   = local_port

        self.remote_hosts = remote_hosts or []
        self.user         = user
        self.keyfile      = keyfile

        self._workers = set()
        self._avail   = Queue()

        self._logger = logging.getLogger(
            f'{self.__class__.__name__}-{str(id(self))[:4]}')
        self._logger.debug('Created self')

        self._create_with = {}

        self.start()

    def __del__(self):
        self._logger.debug('Destroying...')

        self.stop()

    @staticmethod
    def get_or_create():
        return WorkerManager()

    def _add_worker(self, cls, *args, **kwargs):
        w = cls(*args, **kwargs).start()

        self._create_with[w] = (cls, args, kwargs)
        self._workers.add(w)
        self._avail.put(w)

    def start(self):
        self._logger.debug('Manager starting ...')

        for port in range(self.local_port, self.local_port  + self.n_local):
            self._logger.debug(f'Starting worker on port: {port}')
            self._add_worker(LocalWorker, port=port)

        for i, h in enumerate(self.remote_hosts):
            self._logger.debug(f'Starting worker no. {i} at {self.user}@{h}')

            self._add_worker(SSHWorker, host=h, user=self.user, keyfile=self.keyfile)

        return self

    def _put(self, retval,
             worker, db_entry, on_finished):

        self._logger.info(f'ID {db_entry["ID"]} returned: {retval}')

        s3_info = get_s3_info(db_entry=db_entry)

        (local_path,
         s3_path) = s3_info

        safe_s3_sync(s3_path, local_path)

        on_finished()

        self._avail.put(worker)

    def dispatch(self, db_entry, task,
                 upstream_update, on_finished):
        self._logger.debug(f'Dispatch called for (task={id(task)})')

        w = self._avail.get()  # type: SSHWorker

        _put = partial(self._put,
                       worker=w, db_entry=db_entry, on_finished=on_finished)

        self._logger.debug(f'Fetched available worker {w}')

        w.dispatch(callback=_put,
                   upstream_update=upstream_update, db_entry=db_entry,
                   task=task)

        self._logger.debug('Dispatch done')

    def stop(self):
        for w in self._workers:
            self._logger.debug(f'Stopping worker {w}')

            w.stop()
