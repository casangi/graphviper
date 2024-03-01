import os
import glob
import json
import pkgutil
import pathlib
import inspect

import functools
import importlib

import graphviper.utils.logger
import graphviper.utils.console as console

from graphviper.utils.protego import Protego

from typing import Callable, Any, Union, NoReturn, Dict, List, Optional
from types import ModuleType


def is_notebook() -> bool:
    try:
        from IPython import get_ipython

        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True
        else:
            raise ImportError

    except ImportError:
        return False


def validate(
    config_dir: str = None,
    custom_checker: Callable = None,
    add_data_type: Any = None,
    external_logger: Callable = None,
):
    def function_wrapper(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            meta_data = {"function": None, "module": None}
            arguments = inspect.getcallargs(function, *args, **kwargs)

            meta_data["function"] = function.__name__
            meta_data["module"] = function.__module__

            # If this is a class method, drop the self entry.
            if "self" in list(arguments.keys()):
                class_name = args[0].__class__.__name__
                meta_data["function"] = ".".join((class_name, function.__name__))
                del arguments["self"]

            verify(
                function=function,
                args=arguments,
                config_dir=config_dir,
                custom_checker=custom_checker,
                add_data_type=add_data_type,
                external_logger=external_logger,
                meta_data=meta_data,
            )

            return function(*args, **kwargs)

        return wrapper

    return function_wrapper


def get_path(function: Callable) -> str:
    module = inspect.getmodule(function)
    module_path = inspect.getfile(module).rstrip(".py")

    if "src" in module_path:
        # This represents a local developer install
        base_module_path = module_path.split("src/")[0]
        return base_module_path
    else:
        # Here we hope that we can find the package in site-packages and it is unique
        # otherwise the user should provide the configuration path in the decorator.
        base_module_path = module_path.split("site-packages/")[0]
        return "/".join((base_module_path, "site-packages/"))


def config_search(root: str = "/", module_name=None) -> Union[None, str]:
    colorize = console.Colorize()

    if root == "/":
        graphviper.utils.logger.warning(
            "File search from root could take some time ..."
        )

    graphviper.utils.logger.info(
        "Searching {} for configuration file, please wait ...".format(
            colorize.blue(root)
        )
    )

    for file in glob.glob("{root}/**".format(root=root), recursive=True):
        if module_name + ".param.json" in file:
            basename = os.path.dirname(file)
            return basename

    return None


def set_config_directory(path: str, create: bool = False) -> NoReturn:
    if pathlib.Path(path).exists():
        graphviper.utils.logger.info(
            "Setting configuration directory to [{path}]".format(path=path)
        )
        os.environ["PARAMETER_CONFIG_PATH"] = path
    else:
        graphviper.utils.logger.info(
            "The configuration directory [{path}] does not currently exist.".format(
                path=path
            )
        )
        if create:
            graphviper.utils.logger.info(
                "Creating empty configuration directory: {path}".format(path=path)
            )
            pathlib.Path(path).mkdir()


def verify_configuration(path: str, module: ModuleType) -> List[str]:
    modules = []
    for file in glob.glob("{path}/*.param.json".format(path=path), recursive=True):
        if file.endswith(".param.json"):
            modules.append(os.path.basename(file).strip(".param.json"))

    package_path = os.path.dirname(module.__file__)
    package_modules = [name for _, name, _ in pkgutil.iter_modules([package_path])]

    not_found = []
    for module in package_modules:
        if module not in modules:
            not_found.append(module)

    # coverage = (len(package_modules) - len(not_found)) / len(package_modules)

    return package_modules


def verify(
    function: Callable,
    args: Dict,
    meta_data: Dict[str, Union[Optional[str], Any]],
    config_dir: str = None,
    add_data_type: Any = None,
    custom_checker: Callable = None,
    external_logger: Callable = None,
) -> NoReturn:
    colorize = console.Colorize()
    function_name, module_name = meta_data.values()

    # The module call gives the full module chain, but we only want the last
    # module, ex. astrohack.extract_holog would be returned, but we only want
    # extract_holog. This should generally work.

    module_name = module_name.split(".")[-1]

    if external_logger is None:
        logger = graphviper.utils.logger.get_logger()

    else:
        logger = external_logger

    graphviper.utils.logger.info(
        "Checking parameter values for {module}.{function}".format(
            function=colorize.blue(function_name), module=colorize.blue(module_name)
        )
    )

    module_path = get_path(function)
    logger.info(f"Module path: {colorize.blue(module_path)}")

    path = None

    # First we need to find the parameter configuration files
    if config_dir is not None:
        path = config_dir

    # If the parameter configuration directory is not passed as an argument this environment variable should be set.
    # In this case, the environment variable is set in the __init__ file of the astrohack module.
    #
    # This should be set according to the same pattern as PATH in terminal, ie. PATH=$PATH:/path/new
    # the parsing code will expect this.
    elif os.getenv("PARAMETER_CONFIG_PATH"):
        for paths in os.getenv("PARAMETER_CONFIG_PATH").split(":"):
            result = config_search(root=paths, module_name=module_name)
            logger.debug("Result: {}".format(result))
            if result:
                path = result
                logger.debug("PARAMETER_CONFIG_PATH: {dir}".format(dir=result))
                break

        # If we can't find the configuration in the ENV path we will make a last ditch effort to find it in either src/,
        # if that exists or looking in the python site-packages/ directory before giving up.
        if not path:
            logger.info(
                "Failed to find module in PARAMETER_CONFIG_PATH ... attempting to check common directories ..."
            )
            path = config_search(root=module_path, module_name=module_name)

            if not path:
                logger.error(
                    "{function}: Cannot find parameter configuration directory.".format(
                        function=function_name
                    )
                )
                assert False

    else:
        path = config_search(root=module_path, module_name=module_name)
        if not path:
            logger.error(
                "{function}: Cannot find parameter configuration directory.".format(
                    function=function_name
                )
            )
            assert False

    # Define parameter file name
    parameter_file = module_name + ".param.json"

    logger.debug(path + "/" + parameter_file)

    # Load calling module to make this more general
    module = importlib.import_module(function.__module__)

    # This will check the configuration path and return the available modules
    module_config_list = verify_configuration(path, module)

    # Make sure that required module is present
    if module_name not in module_config_list:
        logger.error(
            "Parameter file for {function} not found in {path}".format(
                function=colorize.red(function_name),
                path="/".join((path, parameter_file)),
            )
        )

        raise FileNotFoundError

    with open("/".join((path, parameter_file))) as json_file:
        schema = json.load(json_file)

    if function_name not in schema.keys():
        logger.error(
            "{function} not_found in parameter configuration files.".format(
                function=colorize.format(function_name, color="red", bold=True)
            )
        )

        raise KeyError

    # Here is where the parameter checking is done.
    # First instantiate the validator.
    validator = Protego()

    # Register any additional data types that might be in the configuration file.
    if add_data_type is not None:
        validator.register_data_type(add_data_type)

    # Set up the schema to validate against.
    validator.schema = schema[function_name]

    # If a custom unit custom checker is needed, instantiate the
    # void function in the validator class. In the schema this
    # is used with "check allowed with".
    if custom_checker is not None:
        validator.custom_allowed_function = custom_checker

    assert validator.validate(args), logger.error(validator.errors)
