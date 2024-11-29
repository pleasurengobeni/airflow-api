import unittest
from unittest.mock import patch, mock_open
import os

import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dags.custom_dags.dag_builder import generate_etl_dag_file  # Now this import will work

class TestGenerateETLDagFile(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists", return_value=False)  # Mock exists to simulate directory not existing
    @patch("os.remove")
    @patch("os.makedirs")  # Mock os.makedirs here
    def test_generate_etl_dag_file(self, mock_makedirs, mock_remove, mock_exists, mock_open):
        # Mock configuration data
        config = {
            "indicator_name": "TestIndicator",
            "indicator_code": "TI123",
        }
        config_file = "test_config.json"
        dag_template_path = 'dags/template/etl_template.py.jinja'
    
        # Mock the content of the DAG template
        mock_open.return_value.read.return_value = "<dag_name>_etl template content"
    
        # Call the function
        generate_etl_dag_file(config_file, config)
    
        # Assertions
        # Ensure the file open method was called correctly
        mock_open.assert_any_call(dag_template_path, 'r')
    
        # Ensure file was written with expected content
        expected_dag_content = "<dag_name>_etl template content".replace("<dag_name>", f"{config['indicator_name']}_{config['indicator_code']}")
        mock_open.assert_any_call(os.path.join('dags/etl', f"{config['indicator_name']}_{config['indicator_code']}_etl.py"), 'w')
    
        # Ensure os.makedirs was called to ensure the folder exists
        mock_makedirs.assert_called_once_with('dags/etl', exist_ok=True)  # Check the exact call once
        
        # Log output to help debug if needed
        print(f"mock_makedirs calls: {mock_makedirs.call_args_list}")


if __name__ == '__main__':
    unittest.main()
