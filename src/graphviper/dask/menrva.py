import sys

import distributed
import inspect
import importlib
import importlib.util
import pathlib

import graphviper.utils.logger as logger
import graphviper.utils.console as console

from distributed.diagnostics.plugin import WorkerPlugin

from typing import Callable, Tuple, Dict, Any, Union

colorize = console.Colorize()


class MenrvaClient(distributed.Client):
    """
    This and extended version of the general Dask distributed client that will allow for
    plugin management and more extended features.
    """

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
