import pytest
from jinja2 import Template
import os

@pytest.fixture
def load_template_with_context():
    """Fixture to load and render the Jinja template with context (etl_pipeline)."""
    template_file = 'dags/template/etl_template.py.jinja'
    
    # Ensure the template file exists before proceeding
    assert os.path.exists(template_file), f"Template file {template_file} not found"
    
    with open(template_file, 'r') as f:
        template = Template(f.read())
    
    # Define the value for etl_pipeline (you can modify this as per your requirement)
    context = {
        'etl_pipeline': 'my_etl_pipeline_value'  # Example of expected replacement value
    }
    
    # Render the template with the context
    rendered = template.render(context)
    return rendered

def test_etl_pipeline_variable(load_template_with_context):
    """Test that the 'etl_pipeline' variable is rendered correctly."""
    rendered = load_template_with_context
    
    # Check if the template contains the 'etl_pipeline' placeholder
    assert '{{ etl_pipeline }}' in rendered, "'etl_pipeline' variable not found in the rendered template"
    
    # Check if the 'etl_pipeline' variable is replaced with the expected value
    expected_value = 'my_etl_pipeline_value'  # Replace this with the actual expected value
    assert expected_value in rendered, f"Expected value '{expected_value}' for 'etl_pipeline' not found in the rendered template"




