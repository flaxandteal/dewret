param(
    [Parameter(Mandatory=$true)]
    [string]$EnvName
)

# Display which environment we're setting up
Write-Host "Setting up conda environment: $EnvName" -ForegroundColor Green

# Run conda create command, not sure if fixing the python version here is needed or it gets taken care of by pyproject and pip
Write-Host "Creating conda environment." -ForegroundColor Cyan
conda create -n $EnvName -c conda-forge python=3.11 -y

Write-Host "Activating Environment" -ForegroundColor Cyan
conda activate $EnvName

Write-Host "Pip-installing required and dev packages, set up dev env" -ForegroundColor Cyan
pip install -e ".[test]"

# Install pre-commit hooks
Write-Host "Setting up pre-commit hooks..." -ForegroundColor Cyan
conda install pre-commit -y
pre-commit install

Write-Host "Setup completed successfully!" -ForegroundColor Green