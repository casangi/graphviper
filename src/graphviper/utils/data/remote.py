import os
import pathlib
import shutil
import requests
import zipfile

import graphviper.utils.console as console
import graphviper.utils.logger as logger

from typing import NoReturn


def is_notebook() -> bool:
    """
        Determines if code is running in  jupyter notebook.
    Returns
    -------
        bool

    """
    try:
        from IPython import get_ipython

        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True
        else:
            raise ImportError

    except ImportError:
        return False


def download(file: str, folder: str = ".") -> NoReturn:
    """
        Download tool for data stored on dropbox.
    Parameters
    ----------
    file : str
        Filename as stored on dropbox.
    folder : str
        Destination folder.

    Returns
    -------
        No return
    """
    colorize = console.Colorize()

    # Request file metadata from api
    url = f"https://jhoskins.pythonanywhere.com/meta/{file}"
    response = requests.get(url)

    file_meta_data = response.json()

    if response.status_code != 200:
        logger.error(
            f"Requested file not found or there was a problem with the \
            request: {colorize.red(str(response.status_code))}")

        return

    fullname = file_meta_data["file"]
    id = file_meta_data["id"]
    rlkey = file_meta_data["rlkey"]

    url = "https://www.dropbox.com/scl/fi/{id}/{file}?rlkey={rlkey}".format(
        id=id, file=fullname, rlkey=rlkey
    )

    headers = {"user-agent": "Wget/1.16 (linux-gnu)"}

    response = requests.get(url, stream=True, headers=headers)
    total = int(response.headers.get("content-length", 0))

    fullname = str(pathlib.Path(folder).joinpath(fullname))

    if is_notebook():
        from tqdm.notebook import tqdm
    else:
        from tqdm import tqdm

    with open(fullname, "wb") as fd, tqdm(
            desc=fullname, total=total, unit="iB", unit_scale=True, unit_divisor=1024
    ) as bar:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                size = fd.write(chunk)
                bar.update(size)

    if zipfile.is_zipfile(fullname):
        logger.info(f"Extracting file: {fullname} ...")
        shutil.unpack_archive(filename=fullname, extract_dir=folder)

        # Let's clean up after ourselves
        os.remove(fullname)
