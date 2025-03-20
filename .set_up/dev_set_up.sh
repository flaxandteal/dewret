#!/bin/bash

# Define color variables at the top
GREEN="\033[0;32m"
CYAN="\033[0;36m"
RESET="\033[0m"

# Check if environment name was provided
safe_exit() {
    local exit_code=$1
    echo -e "${RED}Script failed with exit code: $exit_code${RESET}"
    
    # If the script is being sourced, use return; otherwise use exit
    [[ "${BASH_SOURCE[0]}" != "${0}" ]] && return $exit_code || exit $exit_code
}

# Store the environment name from the first parameter
ENV_NAME=$1

# Display which environment we're setting up
echo -e "${GREEN}Setting up conda environment: $ENV_NAME${RESET}"

# Run conda create command, not sure if fixing the python version here is needed or it gets taken care of by pyproject and pip
echo -e "${CYAN}Creating conda environment.${RESET}"
conda create -n $ENV_NAME -c conda-forge python=3.11 -y

echo -e "${CYAN}Activating Environment${RESET}"
conda activate $ENV_NAME

echo -e "${CYAN}Pip-installing required and dev packages, set up dev env${RESET}"
pip install -e ".[test]"

# Install pre-commit hooks
echo -e "${CYAN}Setting up pre-commit hooks...${RESET}"
conda install pre-commit -y
pre-commit install

echo -e "${GREEN}Setup completed successfully!${RESET}"