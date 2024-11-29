import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup

# Define paths
config_folder = 'dags/config'
dag_template_path = 'dags/template/etl_template.py.jinja'
etl_folder = 'dags/etl'

# Ensure the output folder exists
os.makedirs(etl_folder, exist_ok=True)

# Function to generate the DAG file from JSON config
def generate_etl_dag_file(config_file, config, **kwargs):
    # Read the DAG template
    with open(dag_template_path, 'r') as template_file:
        template = template_file.read()
    
    
    # Replace placeholders in the template with values from the config
    file_path=os.path.join(config_folder, config_file)
    print(file_path)
    dag_file_content = template.replace('<config_file>', file_path).replace('<dag_name>', f"{config['indicator_name']}_{config['indicator_code']}_etl")
    
    print(dag_file_content)
    
    # Generate the file path for the new or updated DAG
    dag_filename = f"{config['indicator_name']}_{config['indicator_code']}_etl.py"
    dag_file_path = os.path.join(etl_folder, dag_filename)
    
    if os.path.exists(dag_file_path):
        os.remove(dag_file_path)
    
    # Save the generated DAG Python file
    with open(dag_file_path, 'w') as dag_file_output:
        dag_file_output.write(dag_file_content)
    
    print(f"Generated DAG for {config['indicator_name']} ({config['indicator_code']}) at {dag_file_path}")

# Define the Airflow DAG
with DAG(
    dag_id='generate_etl_dags',
    start_date=datetime(2024, 11, 27),
    schedule_interval='* * * * *',  # Run every minute
    catchup=False,
) as dag:

    # Start Dummy Operator
    start_task = DummyOperator(task_id='start')

    # Define the latestOnly task
    latest_only_task = DummyOperator(task_id='latestOnly')

    # Initialize task groups to group tasks by indicator_name
    indicator_task_groups = {}

    # Loop through all the JSON config files in the config folder
    for config_file in os.listdir(config_folder):
        if config_file.endswith('.json'):
            # Load the JSON config file
            with open(os.path.join(config_folder, config_file)) as f:
                config = json.load(f)

            # Create task group for each unique indicator_name
            indicator_name = config['indicator_name']
            if indicator_name not in indicator_task_groups:
                indicator_task_groups[indicator_name] = TaskGroup(
                    group_id=f"{indicator_name}"
                )

            # # Create a dummy task for the indicator_name
            # indicator_name_task = DummyOperator(
            #     task_id=f"{indicator_name}_dummy",
            #     task_group=indicator_task_groups[indicator_name]
            # )
            indicator_name_task = None
            if f"{indicator_name}_dummy" not in globals():
                # Create a dummy task for the indicator_name if it doesn't exist
                indicator_name_task = DummyOperator(
                    task_id=f"{indicator_name}_dummy",
                    task_group=indicator_task_groups[indicator_name]
                )
            else:
                # If the task already exists, just assign it
                indicator_name_task = globals()[f"{indicator_name}_dummy"]

            # Create a task for each indicator_code under the respective indicator_name
            indicator_code_task = PythonOperator(
                task_id=f"{config['indicator_code']}",  # Corrected to dynamically use the indicator_code
                python_callable=generate_etl_dag_file,
                op_args=[config_file, config],
                task_group=indicator_task_groups[indicator_name],
                provide_context=True
            )

            # Set task dependencies: latestOnly -> start -> indicator_name -> indicator_code
            latest_only_task >> start_task >> indicator_name_task >> indicator_code_task

            print(f"Configured task for {config['indicator_name']} - {config['indicator_code']}")

    # Example of how to group tasks by indicator_name and indicator_code:
    for indicator_name, task_group in indicator_task_groups.items():
        # Tasks for each indicator_name group are already set in the loop above
        pass  # No need for further action as the tasks are already grouped and connected
