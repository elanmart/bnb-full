import json
import logging
import multiprocessing
import os
import threading
import time
import uuid
from functools import partial
from queue import Queue

import dill
import rpyc
from tinydb import where

from bnb.track.utils import States, _nested_update, _capture_config
from ..defaults import (backup_entry_path, goc_db, goc_queue, goc_storage_path,
                        prepare)
from ..track import context

TO_SKIP = {'OK'}


class ExecutionManager:
    def __init__(self, dispatcher=None, name='default',
                 to_skip=TO_SKIP, dry_run=False):

        if dispatcher is None:
            from bnb.dispatch import WorkerManager
            dispatcher = WorkerManager.get_or_create()

        self._logger = logging.getLogger(self.__class__.__name__)

        self._db_lock     = multiprocessing.Lock()
        self._ds_lock     = multiprocessing.Lock()
        self._should_stop = multiprocessing.Event()

        self._dispatcher = dispatcher
        self._dispatched = 0
        self._qname      = name
        self._skip       = to_skip
        self._dry        = dry_run

        self._tasks = Queue(64)

        self._runner = self._start_thread(self._run)

        self._logger.debug(f"Created: {self}")

    def _get_initial_entry(self, ID, rich_id, config):
        self._logger.debug(f'returning entry for (ID={ID}, rich_id={rich_id}')

        name  = rich_id['name']
        entry = {
            'ID'         : ID,
            'rich_id'    : rich_id,
            'status'     : States.PRE_DISPATCH,
            'results'    : {},
            'config'     : config,
            'logs'       : {},
            'storage'    : {
                'root'  : goc_storage_path(ID, name),
                'files' : {}
            },  
            'timing' : {
                'start' : 0,
                'stop'  : 0,
            },
            'misc': {
                'command' : '',
                'host'    : {},
                'error'   : ''
            }
        }

        return entry

    def _start_thread(self, target):
        t = threading.Thread(target=target, daemon=True)
        t.start()

        return t

    def _on_finished(self, ID):
        ep = backup_entry_path(ID)

        with self._ds_lock:
            self._dispatched -= 1

        return

        # if os.path.exists(ep):
        #     self._logger.debug(f'Loading {ep}')
        #
        #     with self._db_lock:
        #
        #         db     = goc_db(ID)
        #         cond   = (where('ID') == ID)
        #
        #         if db.contains(cond):
        #             upsert = partial(db.update, cond=cond)
        #         else:
        #             upsert = db.insert
        #
        #         with open(ep) as f:
        #
        #             entry = json.load(f)
        #             if entry['status'] == States.RUNNING:
        #                 return
        #
        #             upsert(
        #                 entry,
        #             )
        #
        # else:
        #     self._logger.debug(f"No backup entry found at {ep}")

    def _should_skip(self, name, config):

        def test_func(doc):
            return (doc['config'] == config) & (doc['status'] in self._skip)

        with self._db_lock:
            cond = goc_db(name=name).contains(test_func)

        return cond

    def _run(self):
        skipped = 0
        _queue  = goc_queue(self._qname)

        while not self._should_stop.is_set():
            self._logger.debug(f'Waiting for tasks to arrive to global (former local) queue')

            queued = _queue.get()
            rich_id, *task = dill.loads(queued)

            ID       = uuid.uuid4().hex
            name     = rich_id['name']
            config   = _capture_config(*task)
            skip     = self._should_skip(name, config)

            if skip:
                skipped += 1
                self._logger.warning(f'Skipping. So far skipped {skipped}')
                continue

            if self._dry:
                self._logger.debug('Skipping due to dry-run being True')
                continue

            with self._db_lock:
                try:
                    prepare(ID, name)
                except:
                    time.sleep(3)
                    prepare(ID, name)

            with self._ds_lock:
                self._dispatched += 1

            self._logger.info(f'fetched from local queue (task={id(task)}, ID={ID})')

            entry    = self._get_initial_entry(ID, rich_id, config)
            callback = partial(self._on_finished, ID)

            self._insert(entry)
            self._dispatcher.dispatch(db_entry=entry, task=task,
                                      upstream_update=self.update, 
                                      on_finished=callback)

            self._logger.debug(f'Dispatch returned (task={id(task)}, ID={ID})')

    def _insert(self, entry):
        ID = entry['ID']
        self._logger.debug(f'Insert: (ID={ID})')

        with self._db_lock:
            goc_db(ID).insert(entry)

    @staticmethod
    def critical_upadte():
        args = ('status', )
        kwargs = dict(value=States.DEAD)

        return args, kwargs

    def update(self, ID, *path, value, mode='replace'):

        if isinstance(value, bytes):
            value = dill.loads(value)

        if path[0] == 'status':
            self._logger.info(f'Status: {value}')

        self._logger.debug(f'Update: (ID={ID}, path={path}, value={value}')

        def fn(doc):
            _nested_update(doc, *path, value=value, mode=mode)

        with self._db_lock:
            goc_db(ID).update(fn, where('ID') == ID)

    def stop(self):
        self._should_stop.set()

    def wait(self):
        while self._dispatched > 0:
            time.sleep(30)


class ExecutionContext:
    def __init__(self, db_entry, upstream_update, use_backup=True):
        self._logger = logging.getLogger(self.__class__.__name__ + '@' + db_entry['ID'][:5])
        self._logger.debug("Enterered ctor...")

        self._use_backup = use_backup

        self._db_entry        = db_entry
        self._ID              = db_entry['ID']
        self._experiment_name = db_entry['rich_id']['name']  # TODO(elan): this...

        prepare(self._ID, self._experiment_name)

        self._storage         = goc_storage_path(self._ID, self._experiment_name)
        self._upstream_update = rpyc.async(upstream_update)
        self._db_entry_backup = backup_entry_path(self._ID)
        self._report_cache    = {}

        self._logger.debug(f'Created {self}')

    def __repr__(self):
        return f'{self.__class__.__name__}(ID={self._ID}, storage={self._storage})'

    def _update_upstream(self, *path, value, mode):
        self._logger.debug(f'Trying to update upstream (path={path} value={value})')

        if self._upstream_update is None:
            self._logger.debug('Manager was None')
            return

        try:
            self._upstream_update(self._ID, *path, value=dill.dumps(value), mode=mode)
            self._logger.debug('Sent data to manager')

        except EOFError:
            self._logger.warning('Upstream fucked up')
            self._upstream_update = None

    def _update_local(self, *path, value, mode):
        _nested_update(self._db_entry, *path, value=value, mode=mode)

        if self._use_backup:
            with open(self._db_entry_backup, 'w') as f:
                json.dump(self._db_entry, f)

    def _update(self, *path, value, mode='replace'):
        self._logger.debug(f'Trying to update all (path={path} value={value})')

        self._update_local(*path, value=value, mode=mode)
        self._update_upstream(*path, value=value, mode=mode)

    @property
    def storage(self):
        return self._storage

    def report(self, key, value, cmp=None):
        self._logger.debug(f'Reporting => ({key}={value})')

        if (cmp is not None) and (key in self._report_cache):

            cached = self._report_cache[key]
            candidate = cmp(cached, value)

            if candidate == cached:
                return

            value = candidate

        path  = ('results', key)
        self._report_cache[key] = value
        self._update(*path, value=value)

    def open(self, file, mode='r', tags=(), description='', **kwargs):
        f = open(file=file, mode=mode, **kwargs)
        self.touch(file=file, tags=tags, description=description)

        return f

    def touch(self, file, tags=(), description=''):
        f = open(file=file, mode='a+')

        path  = ['storage', 'files', file]
        value = dict(type='file', tags=tags, description=description)

        self._update(*path, value=value)

        return f

    def makedirs(self, name, mode=511, exist_ok=False, tags=(), description=''):
        os.makedirs(name, mode=mode, exist_ok=exist_ok)

        path = ('storage', 'files', name)
        value = dict(type='directory', tags=tags, description=description)

        self._update(*path, value=value)

    def log_scalar(self, tag, value, step):
        self._update('logs', tag, value=(step, value), mode='append')

    def run(self, f, args, kwargs):
        self._logger.info(f"Trying to run f for args={args} kwargs={kwargs}")

        context.set_current_context(self)

        self._update('status', value=States.RUNNING)

        status = States.UNK
        t0 = time.time()

        try:
            f(*args, **kwargs)
            status = States.OK

        except KeyboardInterrupt:
            status = States.SIGINT

        except Exception as e:
            status = States.FAIL
            self._update('misc', 'error', value=str(e)) 

        finally:
            context.set_current_context(None)
            t1 = time.time()

            self._update('timing', value={'start': t0, 'stop': t1})
            self._update('status', value=status)
