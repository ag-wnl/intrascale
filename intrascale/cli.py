"""
CLI interface module for Intrascale.
Provides a beautiful terminal interface for monitoring and controlling the system.
"""

from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich import box
import time
from typing import Dict, Set, Tuple, Any
from .discovery import NodeDiscovery
from .connection import ConnectionManager
from .resource_manager import ResourceManager
from .executor import TaskExecutor

console = Console()

INTRA_ASCII = """
██╗███╗   ██╗████████╗██████╗  █████╗ ███████╗██╗      █████╗ ███████╗
██║████╗  ██║╚══██╔══╝██╔══██╗██╔══██╗██╔════╝██║     ██╔══██╗██╔════╝
██║██╔██╗ ██║   ██║   ██████╔╝███████║█████╗  ██║     ███████║█████╗  
██║██║╚██╗██║   ██║   ██╔══██╗██╔══██║██╔══╝  ██║     ██╔══██║██╔══╝  
██║██║ ╚████║   ██║   ██║  ██║██║  ██║███████╗███████╗██║  ██║███████╗
╚═╝╚═╝  ╚═══╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝╚══════╝╚═╝  ╚═╝╚══════╝
"""

class CLI:
    def __init__(self):
        """Initialize the CLI interface."""
        self.console = Console()
        self.layout = Layout()
        self.layout.split(
            Layout(name="header", size=10),
            Layout(name="body"),
            Layout(name="footer", size=3)
        )
        self.layout["body"].split_row(
            Layout(name="nodes"),
            Layout(name="tasks")
        )
    
    def show_splash(self) -> None:
        """Show the splash screen with ASCII art."""
        self.console.clear()
        self.console.print(Panel(INTRA_ASCII, style="bold blue"))
        self.console.print("\n[bold]Initializing Intrascale...[/bold]")
        time.sleep(2)
    
    def create_nodes_table(self, nodes: Set[Tuple[str, str]], 
                          connections: Dict[str, Any]) -> Table:
        """Create a table showing discovered nodes and their status."""
        table = Table(title="Network Nodes", box=box.ROUNDED)
        table.add_column("Hostname", style="cyan")
        table.add_column("IP Address", style="green")
        table.add_column("Status", style="yellow")
        table.add_column("CPU Usage", style="red")
        table.add_column("Memory Usage", style="magenta")
        
        for hostname, ip in nodes:
            status = "Connected" if hostname in connections else "Discovered"
            cpu_usage = "N/A"
            memory_usage = "N/A"
            
            if hostname in connections:
                conn = connections[hostname]
                hw_info = conn.hardware_info
                cpu_usage = f"{hw_info.get('cpu_percent', 0):.1f}%"
                memory_usage = f"{hw_info.get('memory_percent', 0):.1f}%"
            
            table.add_row(hostname, ip, status, cpu_usage, memory_usage)
        
        return table
    
    def create_tasks_table(self, tasks: Dict[str, Any]) -> Table:
        """Create a table showing current tasks and their status."""
        table = Table(title="Active Tasks", box=box.ROUNDED)
        table.add_column("Task ID", style="cyan")
        table.add_column("Function", style="green")
        table.add_column("Status", style="yellow")
        table.add_column("Node", style="blue")
        table.add_column("Progress", style="magenta")
        
        for task_id, task_info in tasks.items():
            status = task_info.get('status', 'unknown')
            status_style = {
                'pending': 'yellow',
                'running': 'blue',
                'completed': 'green',
                'failed': 'red'
            }.get(status, 'white')
            
            table.add_row(
                task_id,
                task_info.get('function', 'N/A'),
                f"[{status_style}]{status}[/{status_style}]",
                task_info.get('assigned_node', 'N/A'),
                "⏳" if status == 'running' else "✓" if status == 'completed' else "❌" if status == 'failed' else "⏳"
            )
        
        return table
    
    def update_display(self, nodes: Set[Tuple[str, str]], 
                      connections: Dict[str, Any],
                      tasks: Dict[str, Any]) -> None:
        """Update the display with current system status."""
        self.layout["header"].update(Panel(INTRA_ASCII, style="bold blue"))
        self.layout["nodes"].update(self.create_nodes_table(nodes, connections))
        self.layout["tasks"].update(self.create_tasks_table(tasks))
        self.layout["footer"].update(Panel("Press Ctrl+C to exit", style="bold red"))
        
        self.console.clear()
        self.console.print(self.layout)
    
    def run(self, discovery: NodeDiscovery, 
            connection_manager: ConnectionManager,
            resource_manager: ResourceManager) -> None:
        """Run the CLI interface."""
        self.show_splash()
        
        with Live(self.layout, refresh_per_second=1) as live:
            while True:
                nodes = discovery.get_nodes()
                connections = connection_manager.get_connected_nodes()
                tasks = resource_manager.get_all_tasks()
                
                self.update_display(nodes, connections, tasks)
                time.sleep(1) 