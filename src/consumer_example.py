import zmq
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    
    # Connect to the proxy
    proxy_addr = "tcp://127.0.0.1:5555"
    socket.connect(proxy_addr)
    
    # Subscribe to all topics
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    
    logger.info(f"Connected to IB Proxy at {proxy_addr}")
    logger.info("Waiting for data... (Ctrl+C to stop)")
    
    try:
        while True:
            # Receive topic and message
            message = socket.recv_string()
            topic, data_json = message.split(' ', 1)
            data = json.loads(data_json)
            
            print(f"[{topic}] {data}")
            
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        socket.close()
        context.term()

if __name__ == "__main__":
    main()
