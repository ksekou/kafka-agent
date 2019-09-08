class TracingMeta(type):
    @classmethod
    def __prepare__(mcs, name, bases, **kwargs):
        namespace = super().__prepare__(name, bases)
        import pdb

        pdb.set_trace()
        return namespace

    def __new__(mcs, name, bases, namespace, **kwargs):
        cls = super().__new__(mcs, name, bases, namespace)
        import pdb

        pdb.set_trace()
        return cls

    def __init__(cls, name, bases, namespace, **kwargs):
        import pdb

        pdb.set_trace()
        super().__init__(name, bases, namespace)


# ------------------------------------------------------------------


from dataclasses import dataclass


@dataclass
class Item:
    _id: int
    name: str


# app = KafkaAgent(
#     'agent-name',
#     key_value=Item,
#     value_type=Item,
#     broker=['localhost:9092'],
#     config={
#         'producer': {},
#         'consumer': {}
#     },
#     concurrency=5,
#     application='guillotina'
# )

# @app.agent()
# async def agent007(*args, **kwargs):
#     async for msg in stream:
#         print(msg)

# @agent007.on_before_message()
# async def do_something(*args, **kwargs):
#     pass

# @agent007.on_after_message()
# async def do_something(*args, **kwargs):
#     pass

# await agent007.send(value=Item(_id=1, name='test'))
# await app.run()

