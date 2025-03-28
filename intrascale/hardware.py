"""
Hardware information gathering module for Intrascale.
Collects and reports system resources available for distributed computing.
"""

import psutil
import platform
import json
from typing import Dict, Any
from dataclasses import dataclass, asdict

@dataclass
class SystemResources:
    """Represents the hardware resources of a system."""
    cpu_count: int
    cpu_percent: float
    memory_total: int
    memory_available: int
    disk_total: int
    disk_free: int
    system: str
    machine: str
    processor: str

class HardwareInfo:
    def __init__(self):
        """Initialize the hardware information collector."""
        self._update_interval = 1.0  # seconds
        self._last_update = 0
    
    def get_system_info(self) -> Dict[str, Any]:
        """
        Get current system information and resources.
        
        Returns:
            Dict containing system information and resource metrics
        """
        resources = SystemResources(
            cpu_count=psutil.cpu_count(),
            cpu_percent=psutil.cpu_percent(interval=1),
            memory_total=psutil.virtual_memory().total,
            memory_available=psutil.virtual_memory().available,
            disk_total=psutil.disk_usage('/').total,
            disk_free=psutil.disk_usage('/').free,
            system=platform.system(),
            machine=platform.machine(),
            processor=platform.processor()
        )
        
        return asdict(resources)
    
    def get_resource_usage(self) -> Dict[str, float]:
        """
        Get current resource usage percentages.
        
        Returns:
            Dict containing CPU and memory usage percentages
        """
        return {
            'cpu_percent': psutil.cpu_percent(interval=1),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_percent': psutil.disk_usage('/').percent
        }
    
    def is_resource_available(self, required_cpu: float = 0.0, 
                            required_memory: float = 0.0) -> bool:
        """
        Check if required resources are available.
        
        Args:
            required_cpu: Required CPU percentage (0-100)
            required_memory: Required memory percentage (0-100)
            
        Returns:
            bool indicating if resources are available
        """
        current_usage = self.get_resource_usage()
        return (current_usage['cpu_percent'] + required_cpu <= 100 and
                current_usage['memory_percent'] + required_memory <= 100)

if __name__ == "__main__":
    # Example usage
    hw_info = HardwareInfo()
    print("System Information:")
    print(json.dumps(hw_info.get_system_info(), indent=2))
    print("\nResource Usage:")
    print(json.dumps(hw_info.get_resource_usage(), indent=2)) 