"""
Task execution module for Intrascale.
Handles receiving and executing tasks on remote nodes using asyncio.
"""

import asyncio
import logging
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
        self._task_handlers: Dict[str, Callable] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
    
    async def start(self) -> None:
        """Start the task executor."""
        self._running = True
        await self._listen_for_tasks()
        logger.info("Task executor started")
    
    def register_task_handler(self, function: Callable) -> None:
        """
        Register a function as a task handler.
        
        Args:
            function: The function to register
        """
        self._task_handlers[function.__name__] = function
    
    async def _listen_for_tasks(self) -> None:
        """Listen for incoming tasks from connected nodes."""
        while self._running:
            for hostname, conn in self.connection_manager.get_connected_nodes().items():
                if not conn.is_active:
                    continue
                
                try:
                    # Check for incoming messages asynchronously
                    message = await self.connection_manager._receive_message_async(conn.socket)
                    if not message:
                        continue
                    
                    if message.get('type') == 'task':
                        # Create a new task for handling this request
                        task = asyncio.create_task(
                            self._handle_task(conn, message['data'])
                        )
                        self._tasks[message['data']['task_id']] = task
                    elif message.get('type') == 'task_status':
                        await self._handle_status_request(conn, message['data'])
                        
                except Exception as e:
                    logger.error(f"Error processing message from {hostname}: {e}")
                    conn.is_active = False
            
            # Small delay to prevent CPU spinning
            await asyncio.sleep(0.1)
    
    async def _handle_task(self, conn: Any, task_data: Dict[str, Any]) -> None:
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
            
            # Execute the task in a thread pool to prevent blocking
            function = self._task_handlers[function_name]
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, function, *args, **kwargs)
            
            # Send success response
            await self.connection_manager._send_message_async(conn.socket, {
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
            await self.connection_manager._send_message_async(conn.socket, {
                'type': 'task_status',
                'data': {
                    'task_id': task_id,
                    'status': 'failed',
                    'error': str(e)
                }
            })
        finally:
            # Clean up task reference
            self._tasks.pop(task_id, None)
    
    async def _handle_status_request(self, conn: Any, status_data: Dict[str, Any]) -> None:
        """
        Handle a task status request.
        
        Args:
            conn: Connection to the requesting node
            status_data: Status request data
        """
        task_id = status_data['task_id']
        # In a real implementation, we would track running tasks and their status
        # For now, we'll just acknowledge the request
        await self.connection_manager._send_message_async(conn.socket, {
            'type': 'task_status',
            'data': {
                'task_id': task_id,
                'status': 'acknowledged'
            }
        })
    
    async def stop(self) -> None:
        """Stop the task executor."""
        self._running = False
        
        # Cancel all running tasks
        for task in self._tasks.values():
            task.cancel()
        
        # Wait for all tasks to complete
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        
        logger.info("Task executor stopped")

if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    async def main():
        # Create connection manager
        conn_manager = ConnectionManager()
        await conn_manager.start_server_async()
        
        # Create task executor
        executor = TaskExecutor(conn_manager)
        
        # Register example task
        def example_task(x: int) -> int:
            return x * x
        
        executor.register_task_handler(example_task)
        await executor.start()
        
        try:
            # Keep running until interrupted
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            await executor.stop()
            await conn_manager.stop()
    
    asyncio.run(main()) 