import asyncio
import logging
from agent import consume_stuff, config
from models import User, Message

log = logging.getLogger(__name__)

if __name__ == "__main__":
    consume_stuff.configure(**config)
    asyncio.run(consume_stuff.consume())
