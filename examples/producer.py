from kafka_agent.core import ProducerComponent
from models import User, Message


if __name__ == "__main__":
    producer = ProducerComponent(key_type=User, value_type=Message)
    message = Message(id=1, recipient=User(id=7, name="Sekou"), body="Hello there!")
