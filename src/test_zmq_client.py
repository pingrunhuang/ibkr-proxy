import zmq
import json
import sys

def test_command(action, params=None):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://127.0.0.1:5556")
    
    payload = {"action": action}
    if params:
        payload.update(params)
    
    print(f"Sending request: {payload}")
    socket.send_json(payload)
    
    # Wait for response with timeout
    if socket.poll(2000):
        resp = socket.recv_json()
        print(f"Received response: {json.dumps(resp, indent=2)}")
    else:
        print("No response from proxy (is it running?)")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_zmq_client.py <action>")
        sys.exit(1)
    
    test_command(sys.argv[1])
