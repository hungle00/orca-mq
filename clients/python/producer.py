from orcamq import OrcaMQClient

queue = OrcaMQClient(host="localhost", port=5555)
for i in range(20):
    queue.publish(
        stream="hello_stream", 
        payload={"message": f"Hello, OrcaMQ! {i}"}
    )
