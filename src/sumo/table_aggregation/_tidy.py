"""For tidying up temp files"""
import logging
import argparse
import asyncio
from pathlib import Path


def parse_args():
    """Parse arguments to script

    Returns:
        something: the arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "extension", type=str, help="file extension for files to delete"
    )
    return parser.parse_args()


def delete(path):
    """Perform deletion of object

    Args:
        path (str): defention of object id for sumo delete endpoint
    """
    path.unlink()


async def delete_file(path, loop):
    """Delete object

    Args:
        object_id (str): the object id in sumo
    """
    print(path)
    loop.run_in_executor(None, delete, path)


async def killem_all(file_paths, loop):
    """Delete all objects in list

    Args:
        objects (list): list of sumo query hits
    """
    for file_path in file_paths:
        await delete_file(file_path, loop)


def main(extension="parquet", folder=None):
    """Delete all files in certain folder

    Args:
        extension (str, optional): the file extenstion to look for. Defaults to "parquet".
        folder (str, optional): name of folder to look in. Defaults to None
    """
    logger = logging.getLogger("caretaker")
    if folder is None:
        folder = Path().cwd()
    else:
        folder = Path(folder)

    files_to_del = folder.glob(f"*.{extension}")
    death_loop = asyncio.get_event_loop()
    death_loop.run_until_complete(killem_all(files_to_del, death_loop))
    logger.debug("Done with cleaning!")


if __name__ == "__main__":
    args = parse_args()
    main(args.extension)
