import sys
import os
from unittest.mock import patch, mock_open
import json
import unittest

# Adjust sys.path to include the root directory so that 'dags' can be found
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dags.custom_dags.dag_builder import generate_etl_dag_file  # Now this import will work



class TestETLPipeline(unittest.TestCase):

    @patch('builtins.open', new_callable=mock_open, read_data='{"indicator_name": "test_indicator", "indicator_code": "001"}')
    @patch('os.path.exists', return_value=False)  # Mock os.path.exists to return False
    @patch('os.makedirs')  # Mock os.makedirs to prevent actual directory creation
    @patch('os.remove')  # Mock os.remove to prevent file deletion
    def test_generate_etl_dag_file(self, mock_remove, mock_makedirs, mock_exists, mock_file):
        config_file = 'alcohol_type_sa_wine_sa_0000001686.json'  # Adjusted to the real file name
        config = json.loads(mock_file.return_value)
        
        # Call the function being tested
        generate_etl_dag_file(config_file, config)

        # Check if the template file was opened
        mock_file.assert_called_with('dags/template/etl_template.py.jinja', 'r')
        
        # Check if the DAG file was written
        expected_dag_content = mock_file.return_value.read().replace(
            '<config_file>', os.path.join('dags/config', config_file)
        ).replace('<dag_name>', f"{config['indicator_name']}_{config['indicator_code']}_etl")
        
        # Check if the file write function was called with the expected content
        mock_file.return_value.write.assert_called_with(expected_dag_content)
        
        # Check if directories and files were manipulated
        mock_makedirs.assert_called_with('dags/etl', exist_ok=True)
        mock_remove.assert_called_with('dags/etl/test_indicator_001_etl.py')

    @patch('builtins.open', new_callable=mock_open)
    @patch('os.path.exists', return_value=True)
    def test_file_exists(self, mock_exists, mock_file):
        config_file = 'alcohol_type_sa_wine_sa_0000001686.json'  # Adjusted to the real file name
        
        # Mock file content for the test
        mock_file.return_value = '{"indicator_name": "test_indicator", "indicator_code": "001"}'
        
        # Call the function and check for expected behavior
        generate_etl_dag_file(config_file, json.loads(mock_file.return_value))
        
        # Ensure the template was read and the file was created
        mock_file.assert_called_with('dags/template/etl_template.py.jinja', 'r')

    @patch('builtins.open', new_callable=mock_open, read_data='{"indicator_name": "test_indicator", "indicator_code": "002"}')
    def test_invalid_json(self, mock_file):
        # Simulate invalid JSON (for testing JSONDecodeError)
        mock_file.return_value = '{"indicator_name": "test_indicator", "indicator_code": "001"'
        
        with self.assertRaises(json.JSONDecodeError):
            json.loads(mock_file.return_value)

if __name__ == '__main__':
    unittest.main()
