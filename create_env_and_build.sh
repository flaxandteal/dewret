#!/bin/bash

# set -x
set -e

# Function to check command execution status
check_status() {
    if [ $? -ne 0 ]; then
        echo "Error: $1 failed"
        return 1
    fi
}

# Function to safely exit the script
safe_exit() {
    echo "Script execution stopped."
    exit 1
}

if [ $# -ne 1 ]; then
    echo "Usage: $0 <conda-environment-name>"
    safe_exit
fi

ENV_NAME=$1

# Print current shell and conda info
echo "Current shell: $SHELL"
# conda info

#!/bin/bash


# Deactivate any active conda environment
echo "Deactivating any active conda environment..."
conda deactivate
check_status "Conda deactivate" || safe_exit
# Set the expected directory name
expected_dir="Dewret"

# Check if current directory name matches and .git folder exists
if [ "$(basename $(pwd))" != "$expected_dir" ] || [ ! -d .git ]; then
    echo "Current directory is not $expected_dir or it doesn't contain a .git folder"
    safe_exit
fi

# Deactivate any active conda environment
echo "Deactivating any active conda environment..."
conda deactivate
check_status "Conda deactivate" || safe_exit

# Create a new conda environment named ENV_NAME
echo "Creating new conda environment '$ENV_NAME'..."
conda create -n $ENV_NAME -y
check_status "Creating conda environment" || safe_exit

# Activate the ENV_NAME environment
echo "Activating '$ENV_NAME' environment..."
conda activate $ENV_NAME
check_status "Activating conda environment" || safe_exit

# Install pip-tools
echo "Installing pip-tools..."
conda install pip -y
pip install pip-tools
check_status "Installing pip-tools" || safe_exit

# Install pre-commit hooks
echo "Installing pre-commit hooks..."
pip install pre-commit
pre-commit install
check_status "Installing pre-commit hooks" || safe_exit

# Compile requirements from pyproject.toml
# Generate requirements.txt file
echo "Compiling requirements from pyproject.toml...and..."
echo "Generating requirements.txt file..."
pip-compile pyproject.toml > requirements.txt
check_status "Generating requirements.txt" || safe_exit

# Install requirements
echo "Installing requirements..."
pip install -r requirements.txt
check_status "Installing requirements" || safe_exit

# Install the current package in editable mode
echo "Installing current package in editable mode..."
python -m pip install -e .
check_status "Installing package in editable mode" || safe_exit

# Run pytest
echo "Running tests with pytest..."
pip install pytest
pytest tests
check_status "Running tests" || safe_exit

echo "Script completed successfully"