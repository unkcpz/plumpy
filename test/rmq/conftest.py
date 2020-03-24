import pytest

import shortuuid

import kiwipy
from kiwipy import rmq


from . import utils

try:
    import aio_pika
    from async_generator import yield_, async_generator

    # pylint: disable=redefined-outer-name

    @pytest.fixture
    @async_generator
    async def connection():
        conn = await aio_pika.connect_robust('amqp://guest:guest@localhost:5672/')
        await yield_(conn)
        await conn.close()

    @pytest.fixture
    @async_generator
    async def communicator(connection):
        communicator = await new_communicator(connection)
        await yield_(communicator)
        await communicator.disconnect()

except ImportError:
    pass


async def new_communicator(connection, settings=None) -> kiwipy.rmq.RmqCommunicator:
    settings = settings or {}

    message_exchange = "{}.{}".format(__file__, shortuuid.uuid())
    task_exchange = "{}.{}".format(__file__, shortuuid.uuid())
    task_queue = "{}.{}".format(__file__, shortuuid.uuid())

    communicator = rmq.RmqCommunicator(connection,
                                       message_exchange=message_exchange,
                                       task_exchange=task_exchange,
                                       task_queue=task_queue,
                                       testing_mode=True,
                                       **settings)
    await communicator.connect()
    return communicator
