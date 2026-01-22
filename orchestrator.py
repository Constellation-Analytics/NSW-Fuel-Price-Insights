import subprocess
import logging
import os
from datetime import datetime

# ----------------------------------------------------------------------------------------------------
#                                       Setup variables and functions
# ----------------------------------------------------------------------------------------------------

os.makedirs("data and logs", exist_ok=True)
filedatestamp = datetime.now().strftime("_%Y%m%d_%Hh%M")
log_file = f"data and logs/workflow{filedatestamp}.log"

# Set up logging for orchestrator
logging.basicConfig(
  filename=log_file,
  level=logging.INFO,
  format="%(asctime)s - %(levelname)s - %(message)s",
  datefmt="%Y-%m-%d %H:%M:%S"
)

# Create logger with dummy name so it can be scaled later if needed
logger = logging.getLogger('log_dog')

# create a reusable function to call modules with logging and error handling
def run_module(module_path):
    try:
        logger.info(f"Starting {module_path}")
        subprocess.run(["python", module_path], check=True)
        logger.info(f"Finished {module_path}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Module {module_path} failed with exit code {e.returncode}")
    except Exception as e:
        logger.exception(f"Unexpected error running {module_path}: {e}")

# ----------------------------------------------------------------------------------------------------
#                                     Script Body - Start
# ----------------------------------------------------------------------------------------------------

# Run module1.py in its entirety
run_module("modules/1.file_retrieval.py")

# ----------------------------------------------------------------------------------------------------
#                                     Script Body - End
# ----------------------------------------------------------------------------------------------------
