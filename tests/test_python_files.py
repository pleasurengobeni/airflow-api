import sys
import os
import json
from unittest.mock import patch, mock_open
import unittest

# Adjust sys.path to include the root directory so that 'dags' can be found
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dags.custom_dags.dag_builder import generate_etl_dag_file  # Now this import will work

class TestETLPipeline(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open, read_data='{"indicator_name": "wine", "indicator_code": "sa_0000001686"}')
    def test_file_exists(self, mock_file):
        config_file = 'alcohol_type_sa_wine_sa_0000001686.json'
        config = {"indicator_name": "wine", "indicator_code": "sa_0000001686"}

        # Call the function that reads the file
        with patch('os.path.exists', return_value=True):  # Mock os.path.exists to return True
            result = generate_etl_dag_file(config_file, config)

        # Ensure that the file was opened with the correct path
        mock_file.assert_called_with('dags/config/alcohol_type_sa_wine_sa_0000001686.json', 'r')

    @patch("builtins.open", new_callable=mock_open, read_data='{"indicator_name": "wine", "indicator_code": "sa_0000001686"}')
    def test_generate_etl_dag_file(self, mock_file):
        config_file = 'alcohol_type_sa_wine_sa_0000001686.json'
        config = {"indicator_name": "wine", "indicator_code": "sa_0000001686"}

        # Mock os.path.exists to return False so that we can test the file generation logic
        with patch('os.path.exists', return_value=False):
            result = generate_etl_dag_file(config_file, config)

        # Check if the correct path for the generated DAG file was called
        mock_file.assert_called_with('dags/template/etl_template.py.jinja', 'r')

        # Further checks can be done on the file content to verify if the DAG was created correctly.
        # For example, you can mock the 'write' call and check if the DAG content is written to the correct file.
        expected_content = 'dummy content'  # Replace with the expected content of the DAG
        # You would also need to check the write call
        mock_file().write.assert_called_with(expected_content)

if __name__ == "__main__":
    unittest.main()
