#!/bin/bash

# Activate the conda environment for the following commands
# This ensures that the correct python and packages are used.

# Start the server in the background using the conda environment
conda run -n server-client-env python run_server.py &

# Start the client in the foreground using the conda environment
# The container will stay running as long as this script is running
conda run -n server-client-env python run_client.py

# If the client exits, you might want the script to wait for the server
# or handle shutdown gracefully. For now, this is a simple setup.
wait 