import psutil
import pathlib
import graphviper
import concurrent.futures

import graphviper.utils.logger as logger

from typing import NoReturn
from graphviper.utils.console import Colorize


def download(file: str, folder: str = ".", source="local") -> NoReturn:
    """
        Download tool for data stored externally.
    Parameters
    ----------
    file : str
        Filename as stored on external source.
    folder : str
        Destination folder.
    source : str
        File metadata source location.

    Returns
    -------
        No return
    """

    if not pathlib.Path(folder).resolve().exists():
        colorize = Colorize()

        graphviper.utils.logger.info(f"Creating path:{colorize.blue(str(pathlib.Path(folder).resolve()))}")
        pathlib.Path(folder).resolve().mkdir()

    if source == "api":
        graphviper.utils.data.remote.download(file=file, folder=folder)

    elif source == "threaded":

        if not isinstance(file, list):
            file = [file]

        n_threads = get_usable_threads(len(file))
        logger.debug(f"Initializing with {n_threads} threads.")

        print_file_list(file)

        with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
            for f in file:
                executor.submit(
                    graphviper.utils.data.dropbox.download,
                    f,
                    folder
                )

    else:
        graphviper.utils.data.dropbox.download(file=file, folder=folder)


def get_usable_threads(n_files: int) -> int:
    import threading

    available_threads = psutil.cpu_count(logical=True) - threading.active_count()

    if available_threads > n_files:
        return n_files

    return int(available_threads)


def print_file_list(files: list) -> NoReturn:
    from rich.table import Table
    from rich.console import Console
    from rich import box

    console = Console()
    table = Table(show_header=True, box=box.SIMPLE)

    table.add_column("Download List", justify="center")

    for file in files:
        table.add_row(f"[magenta]{file}[/magenta]")

    console.print(table)
