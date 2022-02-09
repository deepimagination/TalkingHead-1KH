# vim : fileencoding=UTF-8 :

from __future__ import absolute_import, division, unicode_literals

import logging
import multiprocessing
import sys
import threading
import traceback
import socket
import os
from pathlib import Path


try:
    import queue
except ImportError:
    import Queue as queue  # Python 2.

    BrokenPipeError = OSError


__version__ = "0.3.1"


def install_mp_handler(logger=None):
    """Wraps the handlers in the given Logger with an MultiProcessingHandler.
    :param logger: whose handlers to wrap. By default, the root logger.
    """
    if logger is None:
        logger = logging.getLogger()

    for i, orig_handler in enumerate(list(logger.handlers)):
        handler = MultiProcessingHandler(
            "mp-handler-{0}".format(i), sub_handler=orig_handler
        )

        logger.removeHandler(orig_handler)
        logger.addHandler(handler)


def uninstall_mp_handler(logger=None):
    """Unwraps the handlers in the given Logger from a MultiProcessingHandler wrapper
    :param logger: whose handlers to unwrap. By defaul, the root logger.
    """
    if logger is None:
        logger = logging.getLogger()

    for handler in logger.handlers:
        if isinstance(handler, MultiProcessingHandler):
            orig_handler = handler.sub_handler
            logger.removeHandler(handler)
            logger.addHandler(orig_handler)


class MultiProcessingHandler(logging.Handler):
    def __init__(self, name, sub_handler=None):
        super(MultiProcessingHandler, self).__init__()

        if sub_handler is None:
            sub_handler = logging.StreamHandler()
        self.sub_handler = sub_handler

        self.setLevel(self.sub_handler.level)
        self.setFormatter(self.sub_handler.formatter)
        self.filters = self.sub_handler.filters

        self.queue = multiprocessing.Queue(-1)
        self._is_closed = False
        # The thread handles receiving records asynchronously.
        self._receive_thread = threading.Thread(target=self._receive, name=name)
        self._receive_thread.daemon = True
        self._receive_thread.start()

    def setFormatter(self, fmt):
        super(MultiProcessingHandler, self).setFormatter(fmt)
        self.sub_handler.setFormatter(fmt)

    def _receive(self):
        try:
            broken_pipe_error = BrokenPipeError
        except NameError:
            broken_pipe_error = socket.error

        while True:
            try:
                if self._is_closed and self.queue.empty():
                    break

                record = self.queue.get(timeout=0.2)
                self.sub_handler.emit(record)
            except (KeyboardInterrupt, SystemExit):
                raise
            except (broken_pipe_error, EOFError):
                break
            except queue.Empty:
                pass  # This periodically checks if the logger is closed.
            except:
                traceback.print_exc(file=sys.stderr)

        self.queue.close()
        self.queue.join_thread()

    def _send(self, s):
        self.queue.put_nowait(s)

    def _format_record(self, record):
        # ensure that exc_info and args
        # have been stringified. Removes any chance of
        # unpickleable things inside and possibly reduces
        # message size sent over the pipe.
        if record.args:
            record.msg = record.msg % record.args
            record.args = None
        if record.exc_info:
            self.format(record)
            record.exc_info = None

        return record

    def emit(self, record):
        try:
            s = self._format_record(record)
            self._send(s)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def close(self):
        if not self._is_closed:
            self._is_closed = True
            self._receive_thread.join(5.0)  # Waits for receive queue to empty.

            self.sub_handler.close()
            super(MultiProcessingHandler, self).close()


class VersionCtrlLogger:
    def __init__(
        self, log_dir="logs/", name="run.log", call_file=None, module_level=False
    ):
        log_dir = Path(log_dir)
        self.module_level = module_level
        if not log_dir.exists():
            os.makedirs(log_dir, exist_ok=True)

        current_version = self.get_log_version(log_dir)
        new_version = current_version + 1
        log_dir = log_dir.joinpath(f"version_{new_version}")
        os.makedirs(log_dir, exist_ok=True)
        self.log_path = log_dir.joinpath(name)

        if not module_level:
            logging.basicConfig(level=logging.INFO, handlers=self.get_handlers())

        else:
            self.logger = logging.getLogger(call_file)
            self.logger.setLevel(logging.INFO)

            for hd in self.get_handlers():
                self.logger.addHandler(hd)

    def get_handlers(self):
        # create formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # create console handler and set level to INFO
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        fh = logging.FileHandler(filename=self.log_path)
        fh.setLevel(logging.INFO)

        # add formatter to ch
        ch.setFormatter(formatter)
        fh.setFormatter(formatter)

        return [ch, fh]

    def get_log_version(self, log_path: Path) -> int:
        current_version = -1
        version_ls = [
            int(path.name.split("_")[-1]) for path in log_path.glob("version_*")
        ]

        if len(version_ls) != 0:
            version_ls.sort()
            current_version = version_ls[-1]

        return current_version

    def info(self, string):
        if self.module_level:
            self.logger.info(string)
        else:
            raise NotImplementedError("not support in functional level")
