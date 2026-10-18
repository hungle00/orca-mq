from orcamq import OrcaMQClient

queue = OrcaMQClient(host="localhost", port=5555)
queue.subscribe(
    stream="hello_stream", 
    group="test_group", 
    callback_fn=lambda msg: print(f"[Subscriber] Received: {msg}")
)
