import logging
import os

logger = logging.getLogger('bnb-context')


class DefaultCtx:
    @property
    def storage(self):
        return os.getcwd()

    def update(self, *args, **kwargs):
        pass

    def report(self, key, value, cmp=None):
        print(f'REPORT: key={key}, value={value}')

    def open(self, *args, **kwargs):
        return open(*args, **kwargs)

    def touch(self, file, *args, **kwargs):
        _ = open(file=file, mode='a+')

    def makedirs(self, *args, **kwargs):
        os.makedirs(*args, **kwargs)

    def log_scalar(self, tag, value, step):
        print(f'SCALAR: tag={tag}  value={value:.4f}  step={step}')

    def run(self, f, args, kwargs):
        return f(*args, **kwargs)


class _Context:
    ctx = DefaultCtx()


def set_current_context(ctx):
    if ctx is None:
        ctx = DefaultCtx()

    logger.debug(f'Setting context: {ctx}')
    _Context.ctx = ctx


def get_current_context():
    logger.debug(f'Returning context: {_Context.ctx}')
    return _Context.ctx