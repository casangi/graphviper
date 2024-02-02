import pathlib
import graphviper

from typing import NoReturn


def download(file: str, folder: str = '.') -> NoReturn:

    if not pathlib.Path(folder).exists():
        pathlib.Path(folder).mkdir()

    graphviper.utils.data.dropbox.download(file=file, folder=folder)