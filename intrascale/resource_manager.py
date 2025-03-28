"""
Resource management module for Intrascale.
Coordinates and manages distributed computing tasks across connected nodes.
"""

import threading
import logging
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass
from .connection import ConnectionManager
from .hardware import HardwareInfo

logger = logging.getLogger(__name__)

@dataclass
class Task:
    """Represents a computing task to be distributed."""
    id: str
    function: Callable
    args: tuple
    kwargs: dict
    required_cpu: float
    required_memory: float
    status: str = "pending"  # pending, running, completed, failed
    result: Any = None
    assigned_node: Optional[str] = None

class ResourceManager:
    def __init__(self, connection_manager: ConnectionManager):
        """
        Initialize the resource manager.
        
        Args:
            connection_manager: ConnectionManager instance for node communication
        """
        self.connection_manager = connection_manager
        self.hardware_info = HardwareInfo()
        self.tasks: Dict[str, Task] = {}
        self._lock = threading.Lock()
        self._running = False
    
    def submit_task(self, function: Callable, *args, 
                   required_cpu: float = 0.0,
                   required_memory: float = 0.0,
                   **kwargs) -> str:
        """
        Submit a task for distributed execution.
        
        Args:
            function: The function to execute
            *args: Positional arguments for the function
            required_cpu: Required CPU percentage (0-100)
            required_memory: Required memory percentage (0-100)
            **kwargs: Keyword arguments for the function
            
        Returns:
            Task ID for tracking
        """
        task_id = f"task_{len(self.tasks)}"
        task = Task(
            id=task_id,
            function=function,
            args=args,
            kwargs=kwargs,
            required_cpu=required_cpu,
            required_memory=required_memory
        )
        
        with self._lock:
            self.tasks[task_id] = task
        
        # Try to schedule the task
        self._schedule_task(task_id)
        return task_id
    
    def _schedule_task(self, task_id: str) -> None:
        """Schedule a task to an available node."""
        task = self.tasks[task_id]
        if task.status != "pending":
            return
        
        # Find available node
        available_node = self._find_available_node(
            task.required_cpu,
            task.required_memory
        )
        
        if available_node:
            self._assign_task_to_node(task_id, available_node)
        else:
            logger.warning(f"No available nodes for task {task_id}")
    
    def _find_available_node(self, required_cpu: float, 
                           required_memory: float) -> Optional[str]:
        """Find a node with sufficient resources."""
        for hostname, conn in self.connection_manager.get_connected_nodes().items():
            if not conn.is_active:
                continue
                
            hw_info = conn.hardware_info
            current_cpu = hw_info.get('cpu_percent', 0)
            current_memory = hw_info.get('memory_percent', 0)
            
            if (current_cpu + required_cpu <= 100 and
                current_memory + required_memory <= 100):
                return hostname
        return None
    
    def _assign_task_to_node(self, task_id: str, node_hostname: str) -> None:
        """Assign a task to a specific node."""
        task = self.tasks[task_id]
        conn = self.connection_manager.get_connected_nodes().get(node_hostname)
        
        if not conn:
            return
        
        try:
            # Send task to node
            self.connection_manager._send_message(conn.socket, {
                'type': 'task',
                'data': {
                    'task_id': task_id,
                    'function': task.function.__name__,
                    'args': task.args,
                    'kwargs': task.kwargs,
                    'required_cpu': task.required_cpu,
                    'required_memory': task.required_memory
                }
            })
            
            task.status = "running"
            task.assigned_node = node_hostname
            
            # Start monitoring task status
            threading.Thread(
                target=self._monitor_task,
                args=(task_id,),
                daemon=True
            ).start()
            
        except Exception as e:
            logger.error(f"Failed to assign task {task_id} to node {node_hostname}: {e}")
            task.status = "failed"
    
    def _monitor_task(self, task_id: str) -> None:
        """Monitor the status of a running task."""
        task = self.tasks[task_id]
        if not task.assigned_node:
            return
        
        conn = self.connection_manager.get_connected_nodes().get(task.assigned_node)
        if not conn:
            return
        
        try:
            while task.status == "running":
                # Request task status
                self.connection_manager._send_message(conn.socket, {
                    'type': 'task_status',
                    'data': {'task_id': task_id}
                })
                
                response = self.connection_manager._receive_message(conn.socket)
                if response and response.get('type') == 'task_status':
                    status_data = response['data']
                    if status_data.get('status') == 'completed':
                        task.status = "completed"
                        task.result = status_data.get('result')
                        break
                    elif status_data.get('status') == 'failed':
                        task.status = "failed"
                        break
                
                threading.Event().wait(1.0)  # Check every second
                
        except Exception as e:
            logger.error(f"Error monitoring task {task_id}: {e}")
            task.status = "failed"
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the current status of a task.
        
        Args:
            task_id: ID of the task to check
            
        Returns:
            Dict containing task status and result if available
        """
        task = self.tasks.get(task_id)
        if not task:
            return None
        
        return {
            'status': task.status,
            'result': task.result,
            'assigned_node': task.assigned_node
        }
    
    def get_all_tasks(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all tasks."""
        return {
            task_id: self.get_task_status(task_id)
            for task_id in self.tasks
        }

if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    # Create connection manager
    conn_manager = ConnectionManager()
    threading.Thread(target=conn_manager.start_server, daemon=True).start()
    
    # Create resource manager
    resource_manager = ResourceManager(conn_manager)
    
    # Example task
    def example_task(x: int) -> int:
        return x * x
    
    # Submit task
    task_id = resource_manager.submit_task(
        example_task,
        5,
        required_cpu=10.0,
        required_memory=20.0
    )
    
    input("Press Enter to exit...")
    conn_manager.stop() 