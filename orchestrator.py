import subprocess
import logging
from datetime import datetime

# ----------------------------------------------------------------------------------------------------
#                                       Setup and Variables
# ----------------------------------------------------------------------------------------------------

filedatestamp = datetime.now().strftime("_%Y%m%d_%Hh%M")

# Set up logging for orchestrator
logging.basicConfig(filename="Data and Logs/workflow.log", level=logging.INFO)

# ----------------------------------------------------------------------------------------------------
#                                     Script Body - Start
# ----------------------------------------------------------------------------------------------------

logging.info("Orchestrator started")

# Run module1.py in its entirety
subprocess.run(["python", "modules/module1.py"], check=True)

logging.info("Orchestrator finished")

# ----------------------------------------------------------------------------------------------------
#                                     Script Body - End
# ----------------------------------------------------------------------------------------------------
