import json
import sys
import asyncio
import aiofiles
import os
import yaml
from pymongo import MongoClient
from bson.json_util import dumps
from google.cloud.pubsub import PublisherClient


_resume_token_file = "/var/lib/cobra/resumeToken.txt"
# https://python-forum.io/thread-4798.html
# check the last comment
# Function Iterate over a cursor
async def aiter(iterator):
    for ele in iterator:
        yield ele


# Creating connection for mongo
def create_mongo_change_stream(conf: dict, resume_token: str) -> MongoClient:
    mongo_url = f"mongodb://{conf['username']}:{conf['password']}@{conf['ip']}:{conf['port']}/{conf['database']}"\
                "?authSource=admin&retryWrites=true"
    print(mongo_url)
    client = MongoClient(mongo_url)
    if resume_token is None:
        stream = client.countly_drill.watch()
    else:
        stream = client.countly_drill.watch(resume_after=resume_token)
    return stream


# Creating pubsub Client
def create_pubsub_publisher(conf: dict) -> PublisherClient:
    # settings = conf["BatchSettings"]
    # batch_settings = types.BatchSettings(max_messages=settings["max_messages"])
    # return PublisherClient(batch_settings)
    return PublisherClient()


# Async Code for publishing msg to the given client
async def client_publish(publish_client: PublisherClient, topic: str, message: dict) -> None:
    message = dumps(message).encode("utf-8")
    publish_client.publish(topic=topic, data=message)


# Async code to handle io
async def write_resume_token(ele: dict) -> None:
    async with aiofiles.open(_resume_token_file, "w") as file:
        await file.write(json.dumps(ele))


# Async Iteration over async iterator
async def mongo_event_publish_messages(topic: str,
                                       stream: MongoClient, client: PublisherClient) -> None:
    try:
        async for ele in aiter(stream):
            await client_publish(client, topic, ele)
            await write_resume_token(ele["_id"])
    except Exception as e:
        print(e)


def main() -> None:
    # Step 1 Read Job Conf
    cobra_conf = "./cobra.yaml"
    # cobra_conf = "cobra.yaml"
    if not os.path.isfile(cobra_conf):
        print(f"Cannot Find {cobra_conf}")
        sys.exit(-1)
    conf = yaml.load(open(cobra_conf, "r"), Loader=yaml.FullLoader)

    # Step 2 Check for Resume Token

    if os.path.isfile(_resume_token_file):
        resume_token = json.loads(open(_resume_token_file, "r").read())
    else:
        resume_token = None

    # Step 2 Establish Mongo ChainStream Connection
    change_stream = create_mongo_change_stream(conf, resume_token)

    # Step 3 Establish Publishing client
    publisher = create_pubsub_publisher(conf)

    topic = f"projects/{conf['project']}/topics/{conf['topic']}"

    loop = asyncio.get_event_loop()

    loop.run_until_complete(mongo_event_publish_messages(topic, change_stream,  publisher))


if __name__ == '__main__':

    main()
