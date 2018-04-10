import logging
import subprocess

import os

from ..defaults import goc_storage_path


def get_s3_info(db_entry):

    ID     = db_entry['ID']
    name   = db_entry['rich_id']['name']
    bucket = db_entry['rich_id']['bucket']

    if bucket is None:
        return None, None

    src  = goc_storage_path(ID, name)
    dest = f's3://{bucket}/{ID}'

    return src, dest


def safe_s3_sync(src, dest, exclude=None):

    me = subprocess.check_output(['whoami']).decode()
    logger = logging.getLogger(f'S3-Syncer @ {me}')

    if (src is None) or (dest is None):
        logger.debug('src or dest was None')
        return

    try:

        py  = os.path.expanduser('~/anaconda3/bin/python')
        cmd = f'{py} -m awscli s3 sync {src} {dest}'
        if exclude is not None:
            cmd += f' --exclude {exclude}'

        logger.debug(f'Executing: {cmd}')

        proc = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        stdout, stderr = proc.communicate()
        stdout, stderr = stdout.decode(), stderr.decode()

        logger.debug(f'Command returned with retcode {proc.returncode}')

        if len(stdout) > 0:
            logger.debug(f'stdout: {stdout}')

        if len(stderr) > 0:
            logger.debug(f'stderr: {stderr}')

        if proc.returncode:
            raise subprocess.CalledProcessError(proc.returncode, cmd, output=stdout, stderr=stderr)

    except subprocess.CalledProcessError as e:
        logger.error(f'Error inside safe_s3_sync: {str(e)}')

    except Exception as e:
        logger.error(f'Unhandled exception inside safe_s3_sync: {str(e)}')