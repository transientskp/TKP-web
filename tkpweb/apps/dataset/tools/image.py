import os
from tkp.database.database import DataBase
from tkp.utility.accessors import DataAccessor
from tkp.utility.accessors import FITSImage
from tkp.utility.accessors import CASAImage


def open_image(url, database=None):
    def get_file_by_type(url):
        if not os.path.exists(url):
            return None
        name, ext = os.path.splitext(url)
        if ext.lower() == '.fits':
            image = FITSImage(url)
        elif ext.lower() == '.img':
            image = AIPSppImage(url)
        return image

    image = None
    if isinstance(url, DataAccessor):
        pass
    elif isinstance(url, basestring):
        image = get_file_by_type(url)
    elif isinstance(url, (int, long)):
        db = database if database else DataBase()
        url = Image(id=url, database=database).url
        image = get_file_by_type(url)
    else:
        raise ValueError("unknown image type")
    return image
