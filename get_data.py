import urllib.request
from pathlib import Path
import logging
import tarfile
import os

logger = logging.getLogger(__name__)

import requests


def ret2(url):

    r = requests.get(url, allow_redirects=True)  # to get content after redirection
    print(r.url)  # 'https://media.readthedocs.org/pdf/django/latest/django.pdf'
    with open("file_name.pdf", "wb") as f:
        f.write(r.content)


def retrieve_source(url, raw_file):
    try:
        my_file = Path(raw_file)
        if my_file.is_file():
            msg = "data already downloaded"
        else:
            msg = "downloading remote data"
            urllib.request.urlretrieve(url, my_file)
        return msg
    except FileNotFoundError as e:
        logger.error("{} is not correct or Not found. ".format(e.filename))
        raise (FileNotFoundError)
    except Exception as e:
        logger.error("Got Exception {} while processing".format(e))
        raise (Exception)


def extract(fname, dest_folder):

    if fname.endswith("tar.gz"):
        tar = tarfile.open(fname, "r:gz")
        tar.extractall(dest_folder)
        tar.close()


if __name__ == "__main__":
    ret2("https://www.dropbox.com/s/82j7p69hzmxsbwr/lastfm-dataset-1K.tar.gz")
