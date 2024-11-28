import pytest
import os

def test_python_code():
    python_files = [f for f in os.listdir('dags/custom_dags') if f.endswith('.py')]
    for file in python_files:
        with open(os.path.join('dags/custom_dags', file)) as f:
            code = f.read()
            assert 'def ' in code, f"Functions are missing in {file}"
