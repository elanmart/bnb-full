# import os
# from argparse import ArgumentParser as ArgParser
#
# from sacred.config.version import bump_version, get_version, to_string
# from sacred.dependencies import get_git_root
# from sacred.observers.tb import TensorboardManager
# from sacred.paths import db
# from sacred.utils import normalize_path
#
# import argparse
#
#
#
# __all__ = ['bump', 'tensorboard', 'print_version']
#
#
# def _autoarg(name, **kwargs):
#     parser = ArgParser()
#     parser.add_argument(name, **kwargs)
#     arg = getattr(parser.parse_args(), name)
#
#     return arg
#
#
# def _bump(part):
#     fname = normalize_path(os.getcwd())
#     git_root, git_hash, _ = get_git_root(fname)
#
#     bump_version(name=git_root, commit=git_hash, part=part)
#
#
# def _clear_queue():
#     raise NotImplementedError
#
#
# def _list_queue():
#     raise NotImplementedError
#
#
# def bump():
#     part = _autoarg('part', choices={'major', 'minor', 'patch'},
#                     help="which part of version should we bump")
#     _bump(part)
#
#
# def tensorboard():
#     parser = ArgParser()
#     parser.add_argument('name', default='.')
#     parser.add_argument('--version', default='x.x.x.x')
#     parser.add_argument('--not-interupted', action='store_false')
#     parser.add_argument('--min-runtime', type=float, default=15)
#     parser.add_argument('--tags-all', nargs='+', default=[])
#     parser.add_argument('--tags-any', nargs='+', default=[])
#
#     args = parser.parse_args()
#
#     name = args.name
#     interrupted = not args.not_interrupted
#
#     if os.path.isdir(args.name):
#         name = normalize_path(args.name)
#         name, *_ = get_git_root(name)
#
#     manager = TensorboardManager(name=name, interrupted=interrupted, min_runtime=args.min_runtime,
#                                  db_path=db.path, table_name=db.runs, version_pattern=args.version,
#                                  tags_all=args.tags_all, tags_any=args.tags_any)
#
#     manager.run()
#
#
# def print_version():
#     v = get_version(get_git_root(os.getcwd()))
#     print(f'version:\n{to_string(v)}')
#
#
#
# def _setup_queue_parser(subparsers):
#     queue_parser = subparsers.add_parser('queue')
#     queue_parser.set_defaults(func=queue)
#
#     queue_parser.add_argument('action', choices={'clear', 'list'})
#
#
# def queue(args):
#     if args.action == 'clear':
#         _clear_queue()
#
#     if args.action == 'list':
#         _list_queue()
#
#
# def bnb():
#     parser     = argparse.ArgumentParser()
#     subparsers = argparser.add_subparsers()
#
#     _setup_queue_parser(subparsers)



