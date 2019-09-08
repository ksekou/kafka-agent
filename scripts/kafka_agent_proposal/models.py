from dataclasses import dataclass, asdict, is_dataclass


@dataclass
class User:
    _id: int
    name: str
    _type: str


@dataclass
class Message:
    _id: int
    _from: User
    body: str


def to_json(data):
    if not is_dataclass(data):
        raise Exception("invalid object")
    return asdict(data)
