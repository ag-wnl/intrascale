"""
Main entry point for the Intrascale application.
"""

import logging
import signal
import sys
from typing import Optional
from .discovery import NodeDiscovery
from .connection import ConnectionManager
from .resource_manager import ResourceManager
from .executor import TaskExecutor

logger = logging.getLogger(__name__)

class Intrascale:
    def __init__(self):
        """Initialize the Intrascale application."""
        self.discovery: Optional[NodeDiscovery] = None
        self.connection_manager: Optional[ConnectionManager] = None
        self.resource_manager: Optional[ResourceManager] = None
        self.executor: Optional[TaskExecutor] = None
        self._running = False
    
    def start(self) -> None:
        """Start the Intrascale application."""
        self._running = True
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Start node discovery
        self.discovery = NodeDiscovery()
        self.discovery.start()
        
        # Start connection manager
        self.connection_manager = ConnectionManager()
        self.connection_manager.start_server()
        
        # Start resource manager
        self.resource_manager = ResourceManager(self.connection_manager)
        
        # Start task executor
        self.executor = TaskExecutor(self.connection_manager)
        self.executor.start()
        
        # Connect to discovered nodes
        self._connect_to_discovered_nodes()
        
        logger.info("Intrascale started successfully")
    
    def register_task(self, function) -> None:
        """
        Register a function as a task that can be executed on remote nodes.
        
        Args:
            function: The function to register
        """
        if self.executor:
            self.executor.register_task_handler(function)
    
    def _connect_to_discovered_nodes(self) -> None:
        """Connect to discovered nodes."""
        if not self.discovery or not self.connection_manager:
            return
        
        for hostname, ip in self.discovery.get_nodes():
            if hostname != self.discovery.hostname:  # Don't connect to self
                self.connection_manager.connect_to_node(hostname, ip)
    
    def stop(self) -> None:
        """Stop the Intrascale application."""
        self._running = False
        
        if self.discovery:
            self.discovery.stop()
        
        if self.connection_manager:
            self.connection_manager.stop()
        
        if self.executor:
            self.executor.stop()
        
        logger.info("Intrascale stopped")

def main():
    """Main entry point."""
    intrascale = Intrascale()
    
    def signal_handler(signum, frame):
        """Handle shutdown signals."""
        logger.info("Received shutdown signal")
        intrascale.stop()
        sys.exit(0)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        intrascale.start()
        
        # Example: Register a task
        def example_task(x: int) -> int:
            return x * x
        
        intrascale.register_task(example_task)
        
        # Keep the main thread alive
        while intrascale._running:
            signal.pause()
            
    except Exception as e:
        logger.error(f"Error running Intrascale: {e}")
        intrascale.stop()
        sys.exit(1)

if __name__ == "__main__":
    main() 