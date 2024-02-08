import pathlib
import graphviper

from typing import NoReturn


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
        graphviper.utils.logger.info(f"Creating path:{str(pathlib.Path(folder).resolve())}")
        pathlib.Path(folder).resolve().mkdir()

    if source == "api":
        graphviper.utils.data.remote.download(file=file, folder=folder)
    else:
        graphviper.utils.data.dropbox.download(file=file, folder=folder)
