import dataclasses


@dataclasses.dataclass
class User:
    id: int
    name: str


@dataclasses.dataclass
class Message:
    id: int
    recipient: User
    body: str
