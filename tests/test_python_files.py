import unittest
from unittest.mock import patch, mock_open, MagicMock
import json
import os


import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dags.custom_dags.dag_builder import generate_etl_dag_file  # Now this import will work
class TestGenerateETLDagFile(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open)  # Mock open function for file operations
    @patch("os.remove")  # Mock os.remove to avoid actually deleting files
    @patch("os.path.exists", return_value=False)  # Mock os.path.exists to simulate file doesn't exist
    def test_generate_etl_dag_file(self, mock_exists, mock_remove, mock_open):
        # Prepare mock configuration
        config = {
            "indicator_name": "TestIndicator",
            "indicator_code": "TI123",
        }
        config_file = "test_config.json"
        
        # Mock the content of the DAG template
        mock_open.return_value.read.return_value = "<dag_name>_etl template content"
        
        # Call the function
        generate_etl_dag_file(config_file, config)
        
        # Check if the open function was called correctly to read the template
        mock_open.assert_any_call('dags/template/etl_template.py.jinja', 'r')
        
        # Check if the correct content would be written to the DAG file
        expected_dag_content = "<dag_name> template content".replace("<dag_name>", f"{config['indicator_name']}_{config['indicator_code']}")
        mock_open.assert_any_call(os.path.join('dags/etl', f"{config['indicator_name']}_{config['indicator_code']}_etl.py"), 'w')
        
        # Ensure os.remove was called (because the file doesn't exist, it should be skipped)
        mock_remove.assert_not_called()
        
        # Ensure the generated content has the expected format and the correct placeholders are replaced
        file_path = os.path.join('dags/config', config_file)
        expected_dag_file_content = expected_dag_content.replace('<config_file>', file_path)
        
        # Check if the file was written with expected content
        handle = mock_open()
        handle.write.assert_called_with(expected_dag_file_content)
        
        print("Test passed, mock interactions as expected.")

if __name__ == "__main__":
    unittest.main()
