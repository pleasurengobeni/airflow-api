import pytest
from jinja2 import Template

def test_jinja_template():
    template_file = 'dags/template/etl_template.py.jinja'
    with open(template_file) as f:
        template = Template(f.read())
        rendered = template.render()  # You can pass any variables you need here
        assert 'etl_pipeline' in rendered, f"Missing 'etl_pipeline' in {template_file}"
