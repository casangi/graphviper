import sys

import distributed
import inspect
import importlib
import importlib.util
import pathlib
import warnings

import graphviper.utils.logger as logger
import graphviper.utils.console as console

from distributed.diagnostics.plugin import WorkerPlugin
from distributed.utils import is_python_shutting_down
from distributed.utils import sync
from distributed.utils import NoOpAwaitable
from distributed.utils import wait_for

from dask.typing import NoDefault, no_default

from contextvars import ContextVar
from contextlib import suppress

from typing import Callable, Tuple, Dict, Any, Union

colorize = console.Colorize()

current_client: Union[ContextVar[distributed.Client], ContextVar[None]] = ContextVar("current_client", default=None)
current_cluster: Union[ContextVar[distributed.LocalCluster], ContextVar[None]] = ContextVar("current_cluster", default=None)


class MenrvaClient(distributed.Client):
    """
    This and extended version of the general Dask distributed client that will allow for
    plugin management and more extended features.
    """

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super(MenrvaClient, cls).__new__(cls)
            current_client.set(cls._instance)

        return cls._instance

    def __enter__(self):
        if not self._loop_runner.is_started():
            self.start()
        if self._set_as_default:
            self._previous_as_current = current_client.set(self)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._previous_as_current:
            try:
                current_client.reset(self._previous_as_current)
            except ValueError as e:
                if not e.args[0].endswith(" was created in a different Context"):
                    raise  # pragma: nocover
                warnings.warn(
                    "It is deprecated to enter and exit the Client context "
                    "manager from different threads",
                    DeprecationWarning,
                    stacklevel=2,
                )
        # Close both local cluster AND client. Can change this to self.close() if that isn't the desired behaviour.
        self.shutdown()

    def shutdown(self):
        """Shut down the connected scheduler and workers

               Note, this may disrupt other clients that may be using the same
               scheduler and workers.

               See Also
               --------
               Client.close : close only this client
               """
        current_client.set(None)
        current_cluster.set(None)

        return self.sync(self._shutdown)

    def close(self, timeout=no_default):
        """Close this client

        Clients will also close automatically when your Python session ends

        If you started a client without arguments like ``Client()`` then this
        will also close the local cluster that was started at the same time.


        Parameters
        ----------
        timeout : number
            Time in seconds after which to raise a
            ``dask.distributed.TimeoutError``

        See Also
        --------
        Client.restart
        """
        current_client.set(None)

        if timeout is no_default:
            timeout = self._timeout * 2
        # XXX handling of self.status here is not thread-safe
        if self.status in ["closed", "newly-created"]:
            if self.asynchronous:
                return NoOpAwaitable()
            return
        self.status = "closing"

        with suppress(AttributeError):
            for pc in self._periodic_callbacks.values():
                pc.stop()

        if self.asynchronous:
            coro = self._close()
            if timeout:
                coro = wait_for(coro, timeout)
            return coro

        sync(self.loop, self._close, fast=True, callback_timeout=timeout)
        assert self.status == "closed"

        if not is_python_shutting_down():
            self._loop_runner.stop()

    @staticmethod
    def call(func: Callable, *args: Tuple[Any], **kwargs: Dict[str, Any]):
        try:
            params = inspect.signature(func).bind(*args, **kwargs)
            return func(*params.args, **params.kwargs)

        except TypeError as e:
            logger.error("There was an error calling the function: {}".format(e))

    @staticmethod
    def instantiate_module(
            plugin: str, plugin_file: str, *args: Tuple[Any], **kwargs: Dict[str, Any]
    ) -> WorkerPlugin:
        """

        Args:
            plugin (str): Name of plugin module.
            plugin_file (str): Name of module file. ** This should be moved into the module itself not passed **
            *args (tuple(Any)): This is any *arg that needs to be passed to the plugin module.
            **kwargs (dict[str, Any]): This is any **kwarg default values that need to be passed to the plugin module.

        Returns:
            Instance of plugin class.
        """
        spec = importlib.util.spec_from_file_location(plugin, plugin_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for member in inspect.getmembers(module, predicate=inspect.isclass):
            plugin_instance = getattr(module, member[0])
            logger.debug("Loading plugin module: {}".format(plugin_instance))
            return MenrvaClient.call(plugin_instance, *args, **kwargs)

    def load_plugin(
            self,
            directory: str,
            plugin: str,
            name: str,
            *args: Union[Tuple[Any], Any],
            **kwargs: Union[Dict[str, Any], Any],
    ):
        plugin_file = ".".join((plugin, "py"))
        if pathlib.Path(directory).joinpath(plugin_file).exists():
            plugin_instance = MenrvaClient.instantiate_module(
                plugin=plugin,
                plugin_file="/".join((directory, plugin_file)),
                *args,
                **kwargs,
            )
            logger.debug(f"{plugin}")
            if sys.version_info.major == 3:
                if sys.version_info.minor > 8:
                    self.register_plugin(plugin_instance, name=name)

                else:
                    self.register_worker_plugin(plugin_instance, name=name)
            else:
                logger.warning("Python version may not be supported.")
        else:
            logger.error(
                "Cannot find plugins directory: {}".format(colorize.red(directory))
            )


def port_is_free(port):
    import socket
    import errno

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        s.bind(("127.0.0.1", port))
    except socket.error as e:
        if e.errno == errno.EADDRINUSE:
            logger.warning("Port is already in use.")
            return False
        else:
            # something else raised the socket.error exception
            logger.exception(e)
            return False

    logger.debug("Socket is free.")
    s.close()

    return True


def close_port(port):
    import psutil
    from psutil import process_iter
    from signal import SIGKILL

    for proc in process_iter():
        try:
            for conns in proc.connections(kind='inet'):
                if conns.laddr.port == port:
                    proc.send_signal(SIGKILL)
                    continue

        except psutil.AccessDenied:
            pass
