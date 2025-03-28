"""
Connection management module for Intrascale.
Handles TCP connections with discovered nodes and message passing.
"""

import socket
import json
import threading
import logging
from typing import Dict, Optional, Callable
from dataclasses import dataclass
from .hardware import HardwareInfo

logger = logging.getLogger(__name__)

@dataclass
class NodeConnection:
    """Represents a connection to a remote node."""
    hostname: str
    ip: str
    port: int
    socket: socket.socket
    hardware_info: Dict
    is_active: bool = True

class ConnectionManager:
    def __init__(self, port: int = 50001):
        """
        Initialize the connection manager.
        
        Args:
            port: TCP port to use for connections
        """
        self.port = port
        self.connections: Dict[str, NodeConnection] = {}
        self.hardware_info = HardwareInfo()
        self._running = False
        self._lock = threading.Lock()
    
    def start_server(self) -> None:
        """Start the TCP server to accept incoming connections."""
        self._running = True
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('', self.port))
        server.listen(5)
        
        while self._running:
            try:
                client, addr = server.accept()
                threading.Thread(
                    target=self._handle_incoming_connection,
                    args=(client, addr),
                    daemon=True
                ).start()
            except Exception as e:
                if self._running:
                    logger.error(f"Error accepting connection: {e}")
                break
    
    def connect_to_node(self, hostname: str, ip: str) -> bool:
        """
        Connect to a discovered node.
        
        Args:
            hostname: Hostname of the node to connect to
            ip: IP address of the node
            
        Returns:
            bool indicating if connection was successful
        """
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((ip, self.port))
            
            # Exchange hardware information
            self._send_message(client, {
                'type': 'handshake',
                'data': self.hardware_info.get_system_info()
            })
            
            response = self._receive_message(client)
            if response and response.get('type') == 'handshake':
                with self._lock:
                    self.connections[hostname] = NodeConnection(
                        hostname=hostname,
                        ip=ip,
                        port=self.port,
                        socket=client,
                        hardware_info=response['data']
                    )
                logger.info(f"Connected to node: {hostname} at {ip}")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to connect to {hostname}: {e}")
            return False
    
    def _handle_incoming_connection(self, client: socket.socket, addr: tuple) -> None:
        """Handle an incoming connection from a node."""
        try:
            message = self._receive_message(client)
            if message and message.get('type') == 'handshake':
                hostname = message['data'].get('hostname')
                if hostname:
                    with self._lock:
                        self.connections[hostname] = NodeConnection(
                            hostname=hostname,
                            ip=addr[0],
                            port=addr[1],
                            socket=client,
                            hardware_info=message['data']
                        )
                    logger.info(f"Accepted connection from: {hostname} at {addr[0]}")
                    
                    # Send our hardware information
                    self._send_message(client, {
                        'type': 'handshake',
                        'data': self.hardware_info.get_system_info()
                    })
        except Exception as e:
            logger.error(f"Error handling incoming connection: {e}")
        finally:
            client.close()
    
    def _send_message(self, sock: socket.socket, message: dict) -> None:
        """Send a JSON message over the socket."""
        try:
            data = json.dumps(message).encode()
            sock.sendall(len(data).to_bytes(4, 'big') + data)
        except Exception as e:
            logger.error(f"Error sending message: {e}")
    
    def _receive_message(self, sock: socket.socket) -> Optional[dict]:
        """Receive a JSON message from the socket."""
        try:
            length = int.from_bytes(sock.recv(4), 'big')
            data = sock.recv(length)
            return json.loads(data.decode())
        except Exception as e:
            logger.error(f"Error receiving message: {e}")
            return None
    
    def get_connected_nodes(self) -> Dict[str, NodeConnection]:
        """Get all currently connected nodes."""
        with self._lock:
            return self.connections.copy()
    
    def stop(self) -> None:
        """Stop the connection manager and close all connections."""
        self._running = False
        with self._lock:
            for conn in self.connections.values():
                try:
                    conn.socket.close()
                except:
                    pass
            self.connections.clear()

if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    manager = ConnectionManager()
    
    # Start server in background
    threading.Thread(target=manager.start_server, daemon=True).start()
    
    # Example: Connect to a node
    # manager.connect_to_node("node1", "192.168.1.100")
    
    input("Press Enter to exit...")
    manager.stop() 