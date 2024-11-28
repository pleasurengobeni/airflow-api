import pytest
import subprocess

def test_dockerfile():
    result = subprocess.run(['docker', 'build', '-f', 'Dockerfile', '.'], capture_output=True, text=True)
    assert result.returncode == 0, f"Docker build failed: {result.stderr}"
