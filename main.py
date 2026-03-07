import os
import sys
import subprocess
import logging

# Set up logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def main():
    """
    Dispatcher script for Cloud Run Jobs.
    Reads TASK_MODULE (e.g., corpreg.fetch_full) and runs it.
    """
    task_module = os.getenv("TASK_MODULE")
    task_args = os.getenv("TASK_ARGS", "")

    if not task_module:
        logger.error("TASK_MODULE environment variable is not set.")
        sys.exit(1)

    logger.info(f"Dispatching task: {task_module} with args: {task_args}")

    # Build the command: python -m <task_module> <task_args>
    cmd = [sys.executable, "-m", task_module]
    if task_args:
        cmd.extend(task_args.split())

    try:
        # Run the task and pipe output to stdout/stderr
        result = subprocess.run(cmd, check=True)
        logger.info(f"Task {task_module} completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Task {task_module} failed with exit code {e.returncode}.")
        sys.exit(e.returncode)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
