from orcamq import OrcaMQClient

queue = OrcaMQClient(host="localhost", port=5555)
queue.subscribe("hello_stream", "test_group", lambda msg: print(f"[Subscriber] Received: {msg}"))