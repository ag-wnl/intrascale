import asyncio
import logging
from ..connection import ConnectionManager
from ..resource_manager import ResourceManager
from ..discovery import NodeDiscovery
from ..executor import TaskExecutor

def example_function(x: int, y: str) -> str:
    """A simple example function that combines a number and string."""
    return f"Number: {x}, String: {y}"

async def connect_to_discovered_nodes(discovery: NodeDiscovery, conn_manager: ConnectionManager):
    """Connect to discovered nodes."""
    for hostname, ip in discovery.get_nodes():
        if hostname != discovery.hostname:  # Don't connect to self
            logger.info(f"Attempting to connect to {hostname} at {ip}")
            try:
                success = await conn_manager.connect_to_node(hostname, ip)
                if success:
                    logger.info(f"Successfully connected to {hostname}")
                else:
                    logger.warning(f"Failed to connect to {hostname}")
            except Exception as e:
                logger.error(f"Error connecting to {hostname}: {e}", exc_info=True)

async def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    try:
        # Create and start node discovery
        discovery = NodeDiscovery()
        discovery.start()
        logger.info("Node discovery started")
        
        # Create connection manager
        conn_manager = ConnectionManager()
        
        # Start server in a background task
        server_task = asyncio.create_task(conn_manager.start_server_async())
        logger.info("Connection manager started")
        
        # Wait a bit for discovery to find nodes
        await asyncio.sleep(2)
        
        # Connect to discovered nodes
        await connect_to_discovered_nodes(discovery, conn_manager)
        
        # Create task executor
        executor = TaskExecutor(conn_manager)
        await executor.start()
        logger.info("Task executor started")
        
        # Register the example function as a task handler
        executor.register_task_handler(example_function)
        logger.info("Registered example function as task handler")
        
        # Create resource manager
        resource_manager = ResourceManager(conn_manager)
        logger.info("Resource manager created")
        
        # Example 1: Basic execution with positional arguments
        logger.info("Submitting task 1...")
        try:
            task_id1 = resource_manager.submit_task(example_function, 42, "hello")
            logger.info(f"Task 1 submitted with ID: {task_id1}")
            await asyncio.sleep(1)  # Give time for task to be processed
            result1 = resource_manager.get_task_status(task_id1)
            logger.info(f"Task 1 result: {result1}")
            print("Example 1 - Positional arguments:", result1)
        except Exception as e:
            logger.error(f"Error in task 1: {e}", exc_info=True)

        # Example 2: Execution with keyword arguments
        logger.info("Submitting task 2...")
        try:
            task_id2 = resource_manager.submit_task(example_function, x=42, y="world")
            logger.info(f"Task 2 submitted with ID: {task_id2}")
            await asyncio.sleep(1)  # Give time for task to be processed
            result2 = resource_manager.get_task_status(task_id2)
            logger.info(f"Task 2 result: {result2}")
            print("Example 2 - Keyword arguments:", result2)
        except Exception as e:
            logger.error(f"Error in task 2: {e}", exc_info=True)

        # Example 3: Execution with mixed arguments
        logger.info("Submitting task 3...")
        try:
            task_id3 = resource_manager.submit_task(example_function, 42, y="mixed")
            logger.info(f"Task 3 submitted with ID: {task_id3}")
            await asyncio.sleep(1)  # Give time for task to be processed
            result3 = resource_manager.get_task_status(task_id3)
            logger.info(f"Task 3 result: {result3}")
            print("Example 3 - Mixed arguments:", result3)
        except Exception as e:
            logger.error(f"Error in task 3: {e}", exc_info=True)
        
        # Keep running for a while to allow tasks to complete
        await asyncio.sleep(5)
        
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
    finally:
        # Cleanup in reverse order of initialization
        try:
            await executor.stop()
            discovery.stop()
            await conn_manager.stop()
            logger.info("All components stopped")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main()) 