from threadpoolctl import threadpool_limits
import time
import signal
from typing import Optional
import warnings
import logging
from logging.handlers import QueueHandler, QueueListener
import multiprocessing.pool

from tqdm import tqdm

from job_pool.tqdm_logger import TqdmToLogger

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
        max_jobs_queued: int = 0,
        write_progress_to_logger: bool = False,
        print_progress_every: int = -1,
        total_jobs: Optional[int] = None,
    ):
        """Creates a JobPool object.

        This class addresses several shortcomings of multiprocessing.Pool:
        - Child processes are killed on Exception, including KeyboardInterrupt.
        - Allow nested pools.
        - Allow logging from the child processes.
        - Add progress bar.

        When used in a GUI, the actual processing runs in a child process to allow 
        user interaction with the GUI itself. In this case, we need a NoDaemonProcess
        to allow this child process to spawn children, which is implemented in the 
        NestablePool class. We also need to pass the logger to these grandchildren 
        processes using the multiprocessing.Queue and QueueListener classes.

        Args:
            processes: number of processes. Defaults to 1.
            warningFilter: level of warnings (https://docs.python.org/3/library/warnings.html#warning-filter). Defaults to "default".
            queue: a multiprocessing.Queue object for log messages from worker threads. If None, a new queue is automatically created. Defaults to None.
            timeout: maximum time out for each job in seconds. Defaults to 10000 (~3 hours).
            maxtasksperchild: number of jobs a process can execute before respawning a new process. If None, the number of jobs is unlimited. Default to None.
            max_jobs_queued: use a bounded queue, i.e. applyAsync calls are blocking if there are more than max_jobs_queued jobs in the queue. Default to 0 (=unbounded queue).
            write_progress_to_logger: by default the tqdm progress is only visible on stdout, enabling this flag also writes tqdm progress to logger. Defaults to False.
            print_progress_every: print progress every n iterations, only used for progress bar. Defaults to -1 (let tqdm decide itself when to print progress).
            total_jobs: total number of jobs, only used for progress bar
        """
        self.timeout = timeout
        self.maxtasksperchild = maxtasksperchild
        self.write_progress_to_logger = write_progress_to_logger

        self.job_queue = multiprocessing.Queue(maxsize=max_jobs_queued)
        tqdm_out = None
        if self.write_progress_to_logger:
            tqdm_out = TqdmToLogger(logger, level=logging.INFO)
        self.progress_bar = tqdm(
            total=total_jobs,
            file=tqdm_out,
            miniters=print_progress_every,
            maxinterval=float("inf"),
        )

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
        self.job_queue.put("dummy")
        r = self.pool.apply_async(f, fargs, *args, **kwargs, callback=self.markJobDone)
        self.results.append(r)

    def markJobDone(self, _):
        self.job_queue.get()
        self.progress_bar.update(1)

    def checkPool(self, printProgressEvery: int = -1):
        """Waits for all jobs to complete.

        Args:
            printProgressEvery (int, deprecated): Update the progress every n iterations. Deprecated, moved to JobPool initialization. Defaults to -1 (let tqdm decide itself).
        """        
        try:
            outputs = list()
            for res in self.results:
                self.checkForTerminatedProcess(res)
                outputs.append(res.get())
            self.stopPool()
            self.progress_bar.close()
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
                raise AbnormalWorkerTerminationError(
                    f"Caught abnormal exit of one of the workers: {proc}"
                )

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
