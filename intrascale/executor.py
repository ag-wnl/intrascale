"""
Task execution module for Intrascale.
Handles receiving and executing tasks on remote nodes.
"""

import threading
import logging
import importlib
import inspect
from typing import Dict, Any, Optional, Callable
from .connection import ConnectionManager
from .hardware import HardwareInfo

logger = logging.getLogger(__name__)

class TaskExecutor:
    def __init__(self, connection_manager: ConnectionManager):
        """
        Initialize the task executor.
        
        Args:
            connection_manager: ConnectionManager instance for node communication
        """
        self.connection_manager = connection_manager
        self.hardware_info = HardwareInfo()
        self._running = False
        self._lock = threading.Lock()
        self._task_handlers: Dict[str, Callable] = {}
    
    def start(self) -> None:
        """Start the task executor."""
        self._running = True
        threading.Thread(target=self._listen_for_tasks, daemon=True).start()
        logger.info("Task executor started")
    
    def register_task_handler(self, function: Callable) -> None:
        """
        Register a function as a task handler.
        
        Args:
            function: The function to register
        """
        self._task_handlers[function.__name__] = function
    
    def _listen_for_tasks(self) -> None:
        """Listen for incoming tasks from connected nodes."""
        while self._running:
            for hostname, conn in self.connection_manager.get_connected_nodes().items():
                if not conn.is_active:
                    continue
                
                try:
                    # Check for incoming messages
                    message = self.connection_manager._receive_message(conn.socket)
                    if not message:
                        continue
                    
                    if message.get('type') == 'task':
                        self._handle_task(conn, message['data'])
                    elif message.get('type') == 'task_status':
                        self._handle_status_request(conn, message['data'])
                        
                except Exception as e:
                    logger.error(f"Error processing message from {hostname}: {e}")
                    conn.is_active = False
    
    def _handle_task(self, conn: Any, task_data: Dict[str, Any]) -> None:
        """
        Handle an incoming task.
        
        Args:
            conn: Connection to the node that sent the task
            task_data: Task data including function name and arguments
        """
        task_id = task_data['task_id']
        function_name = task_data['function']
        args = task_data['args']
        kwargs = task_data['kwargs']
        
        try:
            # Check if we have the required function
            if function_name not in self._task_handlers:
                raise ValueError(f"Unknown function: {function_name}")
            
            # Check resource availability
            if not self.hardware_info.is_resource_available(
                task_data['required_cpu'],
                task_data['required_memory']
            ):
                raise RuntimeError("Insufficient resources")
            
            # Execute the task
            function = self._task_handlers[function_name]
            result = function(*args, **kwargs)
            
            # Send success response
            self.connection_manager._send_message(conn.socket, {
                'type': 'task_status',
                'data': {
                    'task_id': task_id,
                    'status': 'completed',
                    'result': result
                }
            })
            
        except Exception as e:
            logger.error(f"Error executing task {task_id}: {e}")
            # Send failure response
            self.connection_manager._send_message(conn.socket, {
                'type': 'task_status',
                'data': {
                    'task_id': task_id,
                    'status': 'failed',
                    'error': str(e)
                }
            })
    
    def _handle_status_request(self, conn: Any, status_data: Dict[str, Any]) -> None:
        """
        Handle a task status request.
        
        Args:
            conn: Connection to the requesting node
            status_data: Status request data
        """
        task_id = status_data['task_id']
        # In a real implementation, we would track running tasks and their status
        # For now, we'll just acknowledge the request
        self.connection_manager._send_message(conn.socket, {
            'type': 'task_status',
            'data': {
                'task_id': task_id,
                'status': 'acknowledged'
            }
        })
    
    def stop(self) -> None:
        """Stop the task executor."""
        self._running = False
        logger.info("Task executor stopped")

if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    # Create connection manager
    conn_manager = ConnectionManager()
    threading.Thread(target=conn_manager.start_server, daemon=True).start()
    
    # Create task executor
    executor = TaskExecutor(conn_manager)
    
    # Register example task
    def example_task(x: int) -> int:
        return x * x
    
    executor.register_task_handler(example_task)
    executor.start()
    
    input("Press Enter to exit...")
    executor.stop()
    conn_manager.stop() 