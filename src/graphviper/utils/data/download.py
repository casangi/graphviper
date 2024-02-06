import pathlib
import graphviper

from typing import NoReturn


def download(file: str, folder: str = ".") -> NoReturn:
    """
        Download tool for data stored externally.
    Parameters
    ----------
    file : str
        Filename as stored on external source.
    folder : str
        Destination folder.

    Returns
    -------
        No return
    """

    if not pathlib.Path(folder).exists():
        pathlib.Path(folder).mkdir()

    graphviper.utils.data.dropbox.download(file=file, folder=folder)
