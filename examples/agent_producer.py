import asyncio
import logging
from agent import consume_stuff, config
from models import User, Message

log = logging.getLogger(__name__)

if __name__ == "__main__":
    consume_stuff.configure(**config)
    message = Message(id=1, recipient=User(id=7, name="Sekou"), body="Hello there!")
