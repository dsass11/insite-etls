#!/usr/bin/env python3
"""
CLI tool for managing ETL pipelines
"""

import argparse
import os
import sys


# Import local modules
try:
    from pipeline_manager.setup import setup_environment
except ImportError:
    # For local development
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from pipeline_manager.setup import setup_environment


def generate_dag_from_yaml(yaml_path):
    """
    Generate an Airflow DAG file from a YAML configuration.

    Args:
        yaml_path (str): Path to the YAML file
    """
    import yaml

    # Ensure the path exists
    if not os.path.isfile(yaml_path):
        print(f"No YAML file found at {yaml_path}")
        return False

    # Get directory and base filename (without extension)
    pipeline_dir = os.path.dirname(yaml_path)
    base_name = os.path.splitext(os.path.basename(yaml_path))[0]

    # Read YAML file
    try:
        with open(yaml_path, 'r') as file:
            config = yaml.safe_load(file)
    except Exception as e:
        print(f"Error reading YAML file: {e}")
        return False

    # Extract DAG configuration
    dag_id = config.get('dag_id', base_name)
    default_args = config.get('default_args', {})
    schedule_interval = config.get('schedule_interval', '0 0 * * *')
    tasks = config.get('tasks', {})

    # Generate the DAG file content
    dag_content = f"""
# Generated file - Do not edit manually
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
import datetime

# Define default arguments
default_args = {{
    'owner': '{default_args.get("owner", "airflow")}',
    'start_date': datetime.datetime.fromisoformat('{default_args.get("start_date", "2025-01-01")}'),
    'retries': {default_args.get("retries", 1)},
    'retry_delay': datetime.timedelta(seconds={default_args.get("retry_delay_sec", 300)})
}}

# Create the DAG
dag = DAG(
    '{dag_id}',
    default_args=default_args,
    schedule_interval='{schedule_interval}',
    catchup=False
)

# Define tasks
"""

    # Add task definitions
    for task_id, task_config in tasks.items():
        operator = task_config.get('operator', '')

        if 'DummyOperator' in operator or operator.endswith('dummy.DummyOperator'):
            dag_content += f"""
{task_id} = DummyOperator(
    task_id='{task_id}',
    dag=dag
)
"""
        elif 'BashOperator' in operator or operator.endswith('bash.BashOperator'):
            bash_command = task_config.get('bash_command', '')
            dag_content += f"""
{task_id} = BashOperator(
    task_id='{task_id}',
    bash_command=\"\"\"{bash_command}\"\"\",
    dag=dag
)
"""

    # Add task dependencies
    dag_content += "\n# Set task dependencies\n"
    for task_id, task_config in tasks.items():
        dependencies = task_config.get('dependencies', [])
        if dependencies:
            for dep in dependencies:
                dag_content += f"{dep} >> {task_id}\n"

    # Write the DAG file
    dag_file = os.path.join(pipeline_dir, f"{base_name}.dag")
    try:
        with open(dag_file, 'w') as file:
            file.write(dag_content)
        print(f"Successfully generated DAG file: {dag_file}")
        print(f"Successfully generated DAG for {pipeline_dir}")
        return True
    except Exception as e:
        print(f"Error writing DAG file: {e}")
        return False


def generate_command(args):
    """
    Handler for the generate command.

    Args:
        args: The command line arguments
    """
    # If path is a directory, look for YAML files
    if os.path.isdir(args.path):
        # Find all YAML files
        yaml_files = []
        for file in os.listdir(args.path):
            if file.endswith(('.yml', '.yaml')):
                yaml_files.append(os.path.join(args.path, file))

        if not yaml_files:
            print(f"No YAML files found in {args.path}")
            return False

        # Generate DAG for each YAML file
        success = True
        for yaml_file in yaml_files:
            if not generate_dag_from_yaml(yaml_file):
                success = False

        return success

    # If path is a file, generate DAG for it
    elif os.path.isfile(args.path):
        return generate_dag_from_yaml(args.path)

    else:
        print(f"Path {args.path} does not exist")
        return False


def setup_command(args):
    """
    Handler for the setup command.

    Args:
        args: The command line arguments
    """
    return setup_environment(args.pipeline_dir)


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(description="Pipeline management tools")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Generate command
    generate_parser = subparsers.add_parser("generate", help="Generate DAG from YAML")
    generate_parser.add_argument("path", help="Path to YAML file or directory containing YAML files")

    # Setup command
    setup_parser = subparsers.add_parser("setup", help="Set up environment for pipeline")
    setup_parser.add_argument("pipeline_dir", help="Path to pipeline directory")

    args = parser.parse_args()

    if args.command == "generate":
        success = generate_command(args)
    elif args.command == "setup":
        success = setup_command(args)
    else:
        parser.print_help()
        return 1

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())