import os
import pytest

# Define the required files and directories
required_files = [
    'docker-compose.yaml',
    'Dockerfile',
    'requirements.txt',
    'dags/custom_dags/dag_builder.py',
    'dags/template/etl_template.py.jinja',
    '.env'
]

# Check for any JSON files in the 'dags/config' directory
def test_required_files():
    # Check for the required files listed explicitly
    for file_path in required_files:
        assert os.path.exists(file_path), f"Required file or directory {file_path} is missing"
    
    # Check if there is at least one .json file in 'dags/config/'
    json_files = [f for f in os.listdir('dags/config') if f.endswith('.json')]
    assert json_files, "No .json file found in 'dags/config/' directory"
