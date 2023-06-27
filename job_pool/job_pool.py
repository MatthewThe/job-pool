import sys
import time
import signal
from typing import Optional
import warnings
import logging
from logging.handlers import QueueHandler, QueueListener
import multiprocessing.pool
import traceback

logger = logging.getLogger(__name__)


# https://stackoverflow.com/questions/6974695/python-process-pool-non-daemonic
class NoDaemonProcess(multiprocessing.Process):
    # make 'daemon' attribute always return False
    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass

    daemon = property(_get_daemon, _set_daemon)


# We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# because the latter is only a wrapper function, not a proper class.
# https://stackoverflow.com/questions/6974695/python-process-pool-non-daemonic#54304172
class NestablePool(multiprocessing.pool.Pool):
    def Process(self, *args, **kwds):
        proc = super(NestablePool, self).Process(*args, **kwds)
        proc.__class__ = NoDaemonProcess

        return proc


class JobPool:
    def __init__(
        self,
        processes: int = 1,
        warningFilter: str = "default",
        queue: Optional[multiprocessing.Queue] = None,
        timeout: int = 10000,
        maxtasksperchild: Optional[int] = None,
    ):
        """Creates a JobPool object

        In the GUIs, the actual processing runs in a child process to allow user
        interaction with the GUI itself. In this case, we need a
        NoDaemonProcess to allow this child process to spawn children,
        which is implemented in the NestablePool class.
        We also need to pass the logger to these grandchildren processes
        using the multiprocessing.Queue and QueueListener classes.

        Args:
            processes: number of processes. Defaults to 1.
            warningFilter: level of warnings (https://docs.python.org/3/library/warnings.html#warning-filter). Defaults to "default".
            queue: a multiprocessing.Queue object. If None, a new queue is automatically created. Defaults to None.
            timeout: maximum time out for each job in seconds. Defaults to 10000 (~3 hours).
            maxtasksperchild: number of jobs a process can execute before respawning a new process. If None, the number of jobs is unlimited. Default to None.
        """
        self.timeout = timeout
        self.warningFilter = warningFilter
        self.maxtasksperchild = maxtasksperchild

        if not queue and multiprocessing.current_process().name != "MainProcess":
            queue = multiprocessing.Queue()
            queue_listener = QueueListener(queue, logger)
            queue_listener.start()
        self.pool = NestablePool(
            processes,
            worker_init,
            initargs=(self.warningFilter, queue),
            maxtasksperchild=self.maxtasksperchild,
        )

        self.results = []

    def applyAsync(self, f, fargs, *args, **kwargs):
        r = self.pool.apply_async(f, fargs, *args, **kwargs)
        self.results.append(r)

    def checkPool(self, printProgressEvery: int = -1):
        processes = self.pool._pool[:]
        try:
            outputs = list()
            for res in self.results:
                self.checkForTerminatedProcess(res, processes)
                # get a fresh list of processes if workers get respawned every n jobs
                if self.maxtasksperchild:
                    processes = self.pool._pool[:]
                outputs.append(res.get())
                if printProgressEvery > 0 and len(outputs) % printProgressEvery == 0:
                    logger.info(
                        f' {len(outputs)} / {len(self.results)} {"%.2f" % (float(len(outputs)) / len(self.results) * 100)}%'
                    )
            self.pool.close()
            self.pool.join()
            return outputs
        except (KeyboardInterrupt, SystemExit) as e:
            logger.error(f"Caught {e.__class__.__name__}, terminating workers")
            self.pool.terminate()
            self.pool.join()
            sys.exit(1)
        except Exception as e:
            logger.error(f"Caught {e.__class__.__name__}. terminating workers")
            logger.error(traceback.print_exc())
            logger.error(e)
            self.pool.terminate()
            self.pool.join()
            sys.exit(1)

    def checkForTerminatedProcess(self, res, processes):
        start_time = time.time()
        while not res.ready():
            if any(proc.exitcode for proc in processes):
                logger.error("Caught abnormal exit of one of the workers, exiting...")
                for proc in processes:
                    if proc.exitcode:
                        logger.error(f"{proc} {proc.exitcode}")
                sys.exit()

            # wait for one second before checking exit codes again
            res.wait(timeout=1)

            if time.time() - start_time > self.timeout:
                raise TimeoutError


def worker_init(warningFilter: str, queue: Optional[multiprocessing.Queue] = None):
    if queue:
        queueHandler = QueueHandler(queue)
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logger.addHandler(queueHandler)

    # set warningFilter for the child processes
    warnings.simplefilter(warningFilter)

    # causes child processes to ignore SIGINT signal and lets main process handle
    # interrupts instead (https://noswap.com/blog/python-multiprocessing-keyboardinterrupt)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
