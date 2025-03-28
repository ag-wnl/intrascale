"""
Node discovery module for Intrascale.
Handles automatic discovery of devices on the local network.
"""

import socket
import threading
import json
import logging
from typing import Set, Tuple

logger = logging.getLogger(__name__)

class NodeDiscovery:
    def __init__(self, port: int = 50000, broadcast_interval: int = 5):
        """
        Initialize the NodeDiscovery service.
        
        Args:
            port: UDP port to use for discovery
            broadcast_interval: How often to broadcast presence (in seconds)
        """
        self.port = port
        self.broadcast_interval = broadcast_interval
        self.nodes: Set[Tuple[str, str]] = set()
        self.hostname = socket.gethostname()
        self.lock = threading.Lock()
        self._running = False
    
    def broadcast_presence(self) -> None:
        """Broadcast the presence of this node on the network."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            while self._running:
                try:
                    message = json.dumps({"hostname": self.hostname})
                    sock.sendto(message.encode(), ('<broadcast>', self.port))
                    threading.Event().wait(self.broadcast_interval)
                except Exception as e:
                    logger.error(f"Error broadcasting presence: {e}")
                    break
    
    def listen_for_nodes(self) -> None:
        """Listen for other nodes broadcasting their presence."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind(('', self.port))
            while self._running:
                try:
                    data, addr = sock.recvfrom(1024)
                    node_info = json.loads(data.decode())
                    with self.lock:
                        self.nodes.add((node_info['hostname'], addr[0]))
                    logger.info(f"Discovered node: {node_info['hostname']} at {addr[0]}")
                except Exception as e:
                    logger.error(f"Error listening for nodes: {e}")
                    break
    
    def start(self) -> None:
        """Start the node discovery service."""
        self._running = True
        threading.Thread(target=self.broadcast_presence, daemon=True).start()
        threading.Thread(target=self.listen_for_nodes, daemon=True).start()
        logger.info("Node discovery service started")
    
    def stop(self) -> None:
        """Stop the node discovery service."""
        self._running = False
        logger.info("Node discovery service stopped")
    
    def get_nodes(self) -> Set[Tuple[str, str]]:
        """Get the current set of discovered nodes."""
        with self.lock:
            return self.nodes.copy()

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Start discovery service
    discovery = NodeDiscovery()
    discovery.start()
    input("Press Enter to exit...")
    discovery.stop() 