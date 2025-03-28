from intrascale import ResourceManager

# Create a task
def my_task(x):
    return x * x

# Submit the task
task_id = resource_manager.submit_task(
    my_task,
    5,
    required_cpu=10.0,
    required_memory=20.0
)

# Check task status
status = resource_manager.get_task_status(task_id)