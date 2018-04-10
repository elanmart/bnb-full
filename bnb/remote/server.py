import signal
import sys
import os
from threading import Thread

from rpyc.utils.server import ThreadedServer

from bnb.dispatch.service import WorkerService



def timeout(signum, frame):
    raise TimeoutError()
    

signal.signal(signal.SIGALRM, timeout)


def main():
    os.chdir('/tmp/')

    t   = ThreadedServer(WorkerService, hostname="localhost", port=0, reuse_addr=True)
    thd = Thread(target=t.start)
    thd.daemon = True
    thd.start()

    sys.stdout.write("%s\n" % (t.port,))
    sys.stdout.flush()

    try:
        sys.stdin.read()
    finally:
        t.close()
        thd.join()


if __name__ == '__main__':
    main()
