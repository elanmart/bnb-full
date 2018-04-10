import datetime
import os
from typing import List, Iterable, Tuple

import pandas as pd
from collections import defaultdict, Callable, namedtuple

from bnb.track.utils import States
from ..defaults import get_root, goc_db, goc_queue

import ast


def avail() -> List[str]:
    
    root = get_root()
    ret  = []

    for name in os.listdir(root):
        path = os.path.join(root, name)
        if path == goc_queue().path:
            continue

        if os.path.isdir(path):
            n_runs = len(goc_db(name=name))
            msg = f'{name:<16}({n_runs} entries)'

            ret.append(msg)

    return ret


def get(name: str) -> 'Results':
    db = goc_db(name=name)

    return Results(db=db, name=name)


TimeInfo = namedtuple('Time', ('hours', 'mins', 'secs'))
def _to_hours_mins_secs(seconds: int) -> TimeInfo:
    mins, secs  = divmod(seconds, 60)
    hours, mins = divmod(mins, 60)

    return TimeInfo(hours, mins, secs)


def _from_timestamp(t: int) -> str:
    return datetime.datetime.fromtimestamp(t).strftime("%d-%m-%y %H:%M:%S")


class Results:
    def __init__(self, db, df=None, name=None) -> None:
        self.db = db
        self.name = name

        if df is None:
            self.df = self._extract_df()  # type: pd.DataFrame
        else:
            self.df = df.copy()  # type: pd.DataFrame

    def __repr__(self):
        return self.df.__repr__()

    def __str__(self):
        return f'{self.__class__.__name__}({self.name}) [{self.df.shape[0]} entries]'

    def __getitem__(self, index):
        df = self.df.iloc[index]
        return self._from_self(df)

    def _perhaps_extract(self, v):
        try: 
            v = ast.literal_eval(v)
        except:
            pass
        
        return v

    def _extract_df(self) -> pd.DataFrame:

        entries = self.db.all()
        rows    = []

        for e in entries:
            row = {}

            row[('details', 'ID')]     = e['ID']
            row[('details', 'status')] = e['status']
            row[('details', 'start')]  = _from_timestamp(e['timing']['start'])
            row[('details', 'total')]  = (e['timing']['stop'] - e['timing']['start']) / 60

            for group_name in ['rich_id', 'results', 'config', 'misc', 'logs']:
                for k, v in e[group_name].items():
                    
                    row[(group_name, k)] = self._perhaps_extract(v)

            rows.append(row)

        keys = sorted(list({
            k for row in rows
            for k in row.keys()
            }
        ))

        index = pd.MultiIndex.from_tuples(keys)
        df = pd.DataFrame(rows, columns=index)

        return df

    def config(self):
        ret = self.df.config.to_dict('records')
        if len(ret) == 1:
            ret = ret[0]

        return ret

    def reset(self) -> 'Results':
        self.df = self._extract_df()

        return self

    def _check_tags(self, tags: Iterable, f: Callable) -> 'Results':
        
        def _ok(row):
            assigned = row.rich_id.tags
            return f(assigned, tags)

        df = self.df[self.df.apply(_ok, axis=1)]

        return self._from_self(df)

    def _from_self(self, df) -> 'Results':
        return Results(db=self.db, df=df, name=self.name)

    def has_all(self, *tags) -> 'Results':
        def _f(assigned, tags):
            return len(set(assigned) - set(tags)) == 0

        return self._check_tags(tags, _f)

    def has_any(self, *tags) -> 'Results':
        def _f(assigned, tags):
            return len(set(assigned) & set(tags)) > 0

        return self._check_tags(tags, _f)

    def top_k(self, metrics: Iterable[str] = None, k=5, ascending=False) -> 'Results':
        results = self.df['results']

        if metrics is None:
            metrics = list(results.columns)

        sort_by = [('results', c) for c in metrics]
        df      = self.df.sort_values(by=sort_by, ascending=ascending).iloc[:k, :]

        return self._from_self(df)

    def only_ok(self, allowed=(States.OK, )) -> 'Results':

        if len(allowed) == 0:
            return self

        cond = (self.df.details.status == allowed[0])
        for status in allowed[1:]:
            cond |= (self.df.details.status == status)

        df = self.df[cond]

        return self._from_self(df)

    def no_details(self) -> 'Results':
        df = self.df[['results', 'config']]

        return self._from_self(df)

    def compare(self) -> 'Results':

        # noinspection PyUnresolvedReferences
        different = self.df['config'].nunique() > 1

        to_drop = different[different == False].index.values
        to_drop = [('config', c) for c in to_drop]

        df = self.df.drop(to_drop, axis=1)

        return self._from_self(df)

    def extract_from_log(self, *columns, key, discard_steps=True) -> 'Results':

        df = self.df.copy()

        if len(columns) == 0:
            columns = list(df['logs'].columns)

        if isinstance(key, Callable):
            suffix = f'_{key.__name__}'
            extract = key

        else:
            suffix = f'_at_{key}'
            extract = lambda sequence: sequence[key]

        def fn(item):

            if not isinstance(item, Iterable):
                item = [(0, item)]

            if discard_steps:
                item = [value for step, value in item]

            return extract(item)

        for c in columns:
            df[('results', c + suffix)] = df.logs[c].apply(fn)

        return self._from_self(df)

    def dash(self):
        pass

    def tb(self):
        pass
