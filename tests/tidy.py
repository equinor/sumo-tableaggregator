"""Small script for tidying case"""
import argparse
import asyncio
from sumo.wrapper import SumoClient


def parse_args():
    """Parse arguments to script

    Returns:
        something: the arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("case", type=str, help="Name of case")
    parser.add_argument("--sumo_env", type=str, help="Name of sumo env", default="prod")
    args = parser.parse_args()
    return args


def delete(path):
    """Perform deletion of object

    Args:
        path (str): defention of object id for sumo delete endpoint
    """
    sumo.delete(path)


async def delete_object(object_id, loop):
    """Delete object

    Args:
        object_id (str): the object id in sumo
    """
    print(object_id)
    path = f"/objects('{object_id}')"
    loop.run_in_executor(None, delete, path)


async def killem_all(objects, loop):
    """Delete all objects in list

    Args:
        objects (list): list of sumo query hits
    """
    print(f"Delete of {len(objects)}")
    for obj in objects:
        await delete_object(obj["_id"], loop)


if __name__ == "__main__":
    args = parse_args()
    sumo = SumoClient(args.sumo_env)
    query = f"fmu.case.name:{args.case} AND class:table AND fmu.aggregation.operation:*"
    objects_to_del = sumo.get(path="/search", query=query, size=1000)["hits"]["hits"]
    my_loop = asyncio.get_event_loop()
    my_loop.run_until_complete(killem_all(objects_to_del, my_loop))
    print("Done!")
