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
    def __init__(self, *args, **kwargs):
        self.processes = []
        super().__init__(*args, **kwargs)

    def Process(self, *args, **kwds):
        proc = super(NestablePool, self).Process(*args, **kwds)
        proc.__class__ = NoDaemonProcess

        self.processes.append(proc)
        return proc

    def check_for_terminated_processes(self):
        for proc in self.processes:
            if proc.exitcode:
                return proc
        self.processes = [p for p in self.processes if p.exitcode != 0]


class AbnormalPoolTerminationError(Exception):
    "Raised when the pool has a non-zero exitcode"
    pass


class AbnormalWorkerTerminationError(Exception):
    "Raised when the worker has a non-zero exitcode"
    pass


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
        self.maxtasksperchild = maxtasksperchild

        if not queue and multiprocessing.current_process().name != "MainProcess":
            queue = multiprocessing.Queue()
            queue_listener = QueueListener(queue, logger)
            queue_listener.start()
        
        self.pool = NestablePool(
            processes,
            worker_init,
            initargs=(warningFilter, queue),
            maxtasksperchild=self.maxtasksperchild,
        )

        self.results = []

    def applyAsync(self, f, fargs, *args, **kwargs):
        r = self.pool.apply_async(f, fargs, *args, **kwargs)
        self.results.append(r)

    def checkPool(self, printProgressEvery: int = -1):
        try:
            outputs = list()
            for res in self.results:
                self.checkForTerminatedProcess(res)
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
            self.stopPool()
            raise AbnormalPoolTerminationError from None
        except Exception as e:
            logger.error(f"Caught {e.__class__.__name__}. terminating workers")
            logger.error(e, exc_info=1)
            self.stopPool()
            raise AbnormalPoolTerminationError from None

    def stopPool(self):
        self.pool.terminate()
        self.pool.join()

    def checkForTerminatedProcess(self, res):
        start_time = time.time()
        while not res.ready():
            if proc := self.pool.check_for_terminated_processes():
                raise AbnormalWorkerTerminationError(f"Caught abnormal exit of one of the workers: {proc}")

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

    # causes child processes to ignore SIGINT (interrupt) signal and lets main process handle
    # interrupts instead (https://noswap.com/blog/python-multiprocessing-keyboardinterrupt)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
