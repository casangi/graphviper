import psutil
import pathlib
import graphviper
import concurrent.futures

import graphviper.utils.logger as logger

from typing import NoReturn
from graphviper.utils.console import Colorize


def download(file: str, folder: str = ".", source="", n_threads=None) -> NoReturn:
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
    n_threads : int
        Number of threads to use.

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

    elif source == "serial":
        graphviper.utils.data.dropbox.download(file=file, folder=folder)

    else:

        if not isinstance(file, list):
            file = [file]

        if n_threads is None:
            n_threads = get_usable_threads(len(file))

        logger.debug(f"Initializing downloader with {n_threads} threads.")

        print_file_list(file)

        with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
            for _file in file:
                executor.submit(
                    graphviper.utils.data.dropbox.download,
                    _file,
                    folder
                )


def get_usable_threads(n_files: int) -> int:
    # Always leave a single thread resource
    available_threads = psutil.cpu_count(logical=True) - 1

    if available_threads >= n_files:
        return n_files

    return int(available_threads)


def print_file_list(files: list) -> NoReturn:
    from rich.table import Table
    from rich.console import Console
    from rich import box

    console = Console()
    table = Table(show_header=True, box=box.SIMPLE)

    table.add_column("Download List", justify="left")

    for file in files:
        table.add_row(f"[magenta]{file}[/magenta]")

    console.print(table)
