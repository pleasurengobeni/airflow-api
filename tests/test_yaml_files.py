import pytest
import yaml

def test_yaml_files():
    yaml_files = [f for f in os.listdir('dags/config') if f.endswith('.yaml')]
    for file in yaml_files:
        with open(os.path.join('dags/config', file), 'r') as f:
            try:
                data = yaml.safe_load(f)
                assert isinstance(data, dict), f"YAML file {file} is not a valid dictionary"
            except yaml.YAMLError as exc:
                pytest.fail(f"YAML file {file} is not valid: {exc}")
