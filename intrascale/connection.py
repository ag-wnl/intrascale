"""
Connection management module for Intrascale.
Handles TCP connections with discovered nodes and message passing using asyncio.
"""

import asyncio
import json
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
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
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
        self._lock = asyncio.Lock()
    
    async def start_server_async(self) -> None:
        """Start the TCP server to accept incoming connections."""
        self._running = True
        server = await asyncio.start_server(
            self._handle_incoming_connection,
            '',
            self.port,
            reuse_address=True
        )
        logger.info(f"Server started on port {self.port}")
        
        try:
            async with server:
                await server.serve_forever()
        except asyncio.CancelledError:
            logger.info("Server stopped")
    
    async def connect_to_node(self, hostname: str, ip: str) -> bool:
        """
        Connect to a discovered node.
        
        Args:
            hostname: Hostname of the node to connect to
            ip: IP address of the node
            
        Returns:
            bool indicating if connection was successful
        """
        try:
            reader, writer = await asyncio.open_connection(ip, self.port)
            
            # Exchange hardware information
            await self._send_message_async(writer, {
                'type': 'handshake',
                'data': self.hardware_info.get_system_info()
            })
            
            response = await self._receive_message_async(reader)
            if response and response.get('type') == 'handshake':
                async with self._lock:
                    self.connections[hostname] = NodeConnection(
                        hostname=hostname,
                        ip=ip,
                        port=self.port,
                        reader=reader,
                        writer=writer,
                        hardware_info=response['data']
                    )
                logger.info(f"Connected to node: {hostname} at {ip}")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to connect to {hostname}: {e}")
            return False
    
    async def _handle_incoming_connection(self, reader: asyncio.StreamReader, 
                                        writer: asyncio.StreamWriter) -> None:
        """Handle an incoming connection from a node."""
        try:
            message = await self._receive_message_async(reader)
            if message and message.get('type') == 'handshake':
                hostname = message['data'].get('hostname')
                if hostname:
                    async with self._lock:
                        self.connections[hostname] = NodeConnection(
                            hostname=hostname,
                            ip=writer.get_extra_info('peername')[0],
                            port=writer.get_extra_info('peername')[1],
                            reader=reader,
                            writer=writer,
                            hardware_info=message['data']
                        )
                    logger.info(f"Accepted connection from: {hostname}")
                    
                    # Send our hardware information
                    await self._send_message_async(writer, {
                        'type': 'handshake',
                        'data': self.hardware_info.get_system_info()
                    })
        except Exception as e:
            logger.error(f"Error handling incoming connection: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
    
    async def _send_message_async(self, writer: asyncio.StreamWriter, 
                                message: dict) -> None:
        """Send a JSON message over the stream."""
        try:
            data = json.dumps(message).encode()
            writer.write(len(data).to_bytes(4, 'big') + data)
            await writer.drain()
        except Exception as e:
            logger.error(f"Error sending message: {e}")
    
    async def _receive_message_async(self, reader: asyncio.StreamReader) -> Optional[dict]:
        """Receive a JSON message from the stream."""
        try:
            length = int.from_bytes(await reader.readexactly(4), 'big')
            data = await reader.readexactly(length)
            return json.loads(data.decode())
        except Exception as e:
            logger.error(f"Error receiving message: {e}")
            return None
    
    def get_connected_nodes(self) -> Dict[str, NodeConnection]:
        """Get all currently connected nodes."""
        return self.connections.copy()
    
    async def stop(self) -> None:
        """Stop the connection manager and close all connections."""
        self._running = False
        async with self._lock:
            for conn in self.connections.values():
                try:
                    conn.writer.close()
                    await conn.writer.wait_closed()
                except:
                    pass
            self.connections.clear()

if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    async def main():
        manager = ConnectionManager()
        await manager.start_server_async()
        
        try:
            # Keep running until interrupted
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            await manager.stop()
    
    asyncio.run(main()) 