import json
import psutil
import pathlib
import graphviper
import concurrent.futures

import graphviper.utils.logger as logger

from typing import NoReturn, Union
import graphviper.utils.console as console

colorize = console.Colorize()


def version():
    # Load the file dropbox file meta data.
    meta_data_path = pathlib.Path(__file__).parent.joinpath(
        ".dropbox/file.download.json"
    )

    if meta_data_path.exists():
        with open(meta_data_path) as json_file:
            file_meta_data = json.load(json_file)

            print(f'{file_meta_data["version"]}')

    else:
        logger.error(f'Couldn\'t find {colorize.blue(meta_data_path)}.')


def download(file: Union[str, list], folder: str = ".", source="", n_threads=None) -> NoReturn:
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
            n_threads = _get_usable_threads(len(file))

        logger.debug(f"Initializing downloader with {n_threads} threads.")

        _print_file_list(file)

        with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
            for _file in file:
                executor.submit(
                    graphviper.utils.data.dropbox.download,
                    _file,
                    folder
                )


def file_list():
    from rich.table import Table
    from rich.console import Console

    console = Console()

    table = Table(show_header=True, show_lines=True)

    meta_data_path = pathlib.Path(__file__).parent.joinpath(
        ".dropbox/file.download.json"
    )

    with open(meta_data_path) as json_file:
        file_meta_data = json.load(json_file)

        table.add_column("file", style="blue")
        table.add_column("dtype", style="green")
        table.add_column("telescope", style="green")
        table.add_column("size", style="green")
        table.add_column("mode", style="green")

        for filename in file_meta_data["metadata"].keys():
            values = [filename]

            for key, value in file_meta_data["metadata"][filename].items():
                if key in ["dtype", "telescope", "size", "mode"]:
                    values.append(value)

            table.add_row(*values)

    console.print(table)


def _get_usable_threads(n_files: int) -> int:
    # Always leave a single thread resource
    available_threads = psutil.cpu_count(logical=True) - 1

    if available_threads >= n_files:
        return n_files

    return int(available_threads)


def _print_file_list(files: list) -> NoReturn:
    from rich.table import Table
    from rich.console import Console
    from rich import box

    console = Console()
    table = Table(show_header=True, box=box.SIMPLE)

    table.add_column("Download List", justify="left")

    for file in files:
        table.add_row(f"[magenta]{file}[/magenta]")

    console.print(table)
