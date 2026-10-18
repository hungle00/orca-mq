import socket
import json

class OrcaMQClient:
    def __init__(self, host="localhost", port=5555):
        self.host = host
        self.port = port

    def publish(self, stream: str, payload: dict) -> str:
        """Send a message to the broker and return its message ID."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port))
            s.sendall(f"PUB {stream} {json.dumps(payload)}\n".encode())
            res = s.makefile('r').readline()
            return res.strip().split(" ")[1]  # Return the message ID (for example, "OK 12345").

    def subscribe(self, stream: str, group: str, callback_fn):
        """
        Subscribe to messages from a stream through a consumer group.
        Keep the TCP connection open and call callback_fn(msg) when data arrives.
        """
        while True:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((self.host, self.port))
                    
                    # Send the SUB command to the broker.
                    s.sendall(f"SUB {stream} {group}\n".encode())
                    
                    f = s.makefile('r')
                    
                    # Read the server's subscription confirmation, if available.
                    # ack = f.readline()
                    
                    # Listen for messages streamed from the broker.
                    while True:
                        line = f.readline()
                        if not line:
                            print("[Client] Mất kết nối tới broker, đang thử kết nối lại...")
                            break
                        
                        line = line.strip()
                        if not line:
                            continue
                        
                        # Expected message format sent by the broker:
                        # MSG <stream> <msg_id> <json_payload>
                        parts = line.split(" ", 3)
                        if parts[0] == "MSG" and len(parts) >= 4:
                            _, stream_name, msg_id, payload_str = parts
                            try:
                                payload = json.loads(payload_str)
                                msg = {
                                    "stream": stream_name,
                                    "msg_id": msg_id,
                                    "payload": payload
                                }
                                # Invoke the callback provided by the user.
                                callback_fn(msg)

                                # Automatically ACK after the callback completes successfully.
                                ack_cmd = f"ACK {stream_name} {group} {msg_id}\n"
                                # s.sendall(ack_cmd.encode())
                            except json.JSONDecodeError:
                                print(f"[Client] Lỗi parse JSON payload: {payload_str}")
                        elif parts[0] == "PING":
                            # Handle heartbeats if the broker supports ping/pong.
                            s.sendall(b"PONG\n")
                            
            except (socket.error, ConnectionRefusedError) as e:
                import time
                print(f"[Client] Không thể kết nối đến broker ({e}), thử lại sau 3 giây...")
                time.sleep(3)
