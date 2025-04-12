#!/usr/bin/env python3
"""
Environment setup module for pipeline management
"""

import os
import sys
import subprocess
import time


def run_command(cmd, cwd=None, env=None, check=True):
    """
    Run a shell command and return the result

    Args:
        cmd (list): Command to run
        cwd (str): Current working directory
        env (dict): Environment variables
        check (bool): Whether to check for command success

    Returns:
        bool: True if command succeeded, False otherwise
    """
    try:
        subprocess.run(
            cmd,
            cwd=cwd,
            env=env,
            check=check,
            text=True,
            capture_output=True
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {' '.join(cmd)}")
        print(f"Error output: {e.stderr}")
        return False


def setup_environment(pipeline_dir):
    """
    Set up the Docker environment for a pipeline

    Args:
        pipeline_dir (str): Path to the pipeline directory

    Returns:
        bool: True if setup succeeded, False otherwise
    """
    print(f"Setting up environment for pipeline: {pipeline_dir}")

    # Get absolute path of pipeline directory
    pipeline_abs_path = os.path.abspath(pipeline_dir)

    # Prepare env file to shared config bridge between docker and setup
    pipeline_data_abs = os.path.abspath(os.path.join(pipeline_dir, "data"))
    project_root = os.path.dirname(os.path.dirname(pipeline_abs_path))  # goes up to pipeline-manager
    docker_dir = os.path.join(project_root, "docker")
    env_file_path = os.path.join(docker_dir, ".env")

    with open(env_file_path, "w") as env_file:
        env_file.write(f"PIPELINE_DATA_PATH={pipeline_data_abs}\n")

    # Get path to the docker directory relative to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(os.path.dirname(script_dir))
    docker_dir = os.path.join(base_dir, "pipeline-manager", "docker")

    if not os.path.isdir(docker_dir):
        print(f"Error: Docker directory not found at {docker_dir}")
        return False

    # Find DAG file in pipeline directory
    dag_file = None
    for file in os.listdir(pipeline_abs_path):
        if file.endswith('.dag'):
            dag_file = os.path.join(pipeline_abs_path, file)
            break

    if not dag_file:
        print(f"Error: No DAG file found in {pipeline_dir}")
        return False

    # Extract DAG ID from the filename
    dag_id = os.path.splitext(os.path.basename(dag_file))[0]
    print(f"Found DAG file: {os.path.basename(dag_file)}")
    print(f"Using DAG ID: {dag_id}")

    # Check for data, SQL, and jar directories
    data_dir = os.path.abspath(os.path.join(pipeline_abs_path, "data"))
    print(f"----->data dir is:<--------- {data_dir}")
    if not os.path.isdir(data_dir) or not os.listdir(data_dir):
        print(f"Warning: Data directory {data_dir} not found or empty. Using pipeline folder as data volume.")
        data_dir = pipeline_abs_path

    sql_dir = os.path.join(pipeline_abs_path, "sql")
    has_sql = os.path.isdir(sql_dir) and any(f.endswith('.sql') for f in os.listdir(sql_dir))
    if not has_sql:
        print(f"Warning: SQL directory {sql_dir} not found or contains no SQL files. No SQL initialization will be performed.")

    jar_dir = os.path.join(pipeline_abs_path, "jar")
    has_jar = os.path.isdir(jar_dir) and any(f.endswith('.jar') for f in os.listdir(jar_dir))
    if not has_jar:
        print(f"Warning: Jar directory {jar_dir} not found or contains no JAR files.")

    # Create a dags directory in the docker folder if it doesn't exist
    dags_dir = os.path.join(docker_dir, "dags")
    os.makedirs(dags_dir, exist_ok=True)

    # Copy and rename DAG file to .py if needed
    target_dag_file = os.path.join(dags_dir, f"{dag_id}.py")
    print(f"Copying DAG file to {target_dag_file}")
    with open(dag_file, 'r') as src_file:
        dag_content = src_file.read()

    with open(target_dag_file, 'w') as dst_file:
        dst_file.write(dag_content)

    # Compose file path
    compose_file = os.path.join(docker_dir, "docker-compose-airflow-spark.yml")
    if not os.path.isfile(compose_file):
        print(f"Error: Docker Compose file not found at {compose_file}")
        return False

    # === Setup Phase ===
    print("\n==== SETUP PHASE ====")
    print("Starting Docker Compose environment...")

    # Shut down any existing containers
    if not run_command(["docker", "compose", "-f", compose_file, "down", "-v"], cwd=docker_dir):
        print("Failed to stop existing containers.")
        return False

    # Start Hive Metastore
    print("Starting Hive Metastore...")
    if not run_command(["docker", "compose", "-f", compose_file, "up", "-d", "hive-metastore"], cwd=docker_dir):
        print("Failed to start Hive Metastore.")
        return False

    print("Waiting for Hive Metastore to start...")
    time.sleep(15)  # Give some time for the service to start

    # === Initialization Phase ===
    print("\n==== INITIALIZATION PHASE ====")

    # Run pipeline-specific SQL if it exists
    if has_sql:
        print("Running pipeline-specific SQL files...")
        for sql_file in sorted(f for f in os.listdir(sql_dir) if f.endswith('.sql')):
            print(f"  Executing {sql_file}...")

            # Prepare volume mounts - include jar mount if it exists
            print(f"----->data dir 2 is:<--------- {data_dir}")
            volume_mounts = [
                "-v", f"{os.path.join(sql_dir, sql_file)}:/app/sql/pipeline.sql",
                "-v", f"{data_dir}:/app/data"
            ]

            if has_jar:
                volume_mounts.extend(["-v", f"{jar_dir}:/app/jar"])

            pipeline_sql_cmd = [
                "docker", "compose", "-f", compose_file, "run", "--rm",
                *volume_mounts,
                "spark-runner", "bash", "-c",
                "/opt/spark/bin/spark-sql --master local[1] "
                "--conf spark.sql.catalogImplementation=hive "
                "--conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 "
                "-f /app/sql/pipeline.sql"
            ]

            if not run_command(pipeline_sql_cmd, cwd=docker_dir):
                print(f"Warning: Failed to execute {sql_file}. Continuing with next file.")

    # === Airflow Setup Phase ===
    print("\n==== AIRFLOW SETUP PHASE ====")
    print("Starting Airflow services...")

    # Ensure the dags directory is mounted to Airflow
    # This is typically handled in the docker-compose file
    os.makedirs(os.path.join(docker_dir, "dags"), exist_ok=True)

    # If we have a jar directory, make sure it's available to the spark-runner container
    if has_jar:
        # Copy JAR files to a location that will be mounted in the docker-compose.yml
        jars_dir = os.path.join(docker_dir, "jars")
        os.makedirs(jars_dir, exist_ok=True)

        print(f"Copying JAR files from {jar_dir} to {jars_dir}...")
        for jar_file in (f for f in os.listdir(jar_dir) if f.endswith('.jar')):
            src_jar = os.path.join(jar_dir, jar_file)
            dst_jar = os.path.join(jars_dir, jar_file)
            print(f"  Copying {jar_file}...")

            # Read and write the jar file
            with open(src_jar, 'rb') as src:
                with open(dst_jar, 'wb') as dst:
                    dst.write(src.read())

    # Start Airflow services
    airflow_cmd = [
        "docker", "compose", "-f", compose_file, "up", "-d",
        "airflow-webserver", "airflow-scheduler", "spark-runner"
    ]

    if not run_command(airflow_cmd, cwd=docker_dir):
        print("Failed to start Airflow services.")
        return False

    print("Waiting for Airflow to initialize...")
    time.sleep(20)  # Give Airflow time to initialize

    # === Final Instructions ===
    print("\n==== ENVIRONMENT READY ====")
    print(f"DAG ID: {dag_id}")
    print("Airflow UI available at: http://localhost:8081")
    print("Username: admin")
    print("Password: admin")
    print("\nTo run your DAG, go to the Airflow UI and manually trigger it.")
    print("\n==== CLEANUP ====")
    print("When finished, run this command to stop all services:")
    print(f"cd {docker_dir} && docker compose -f docker-compose-airflow-spark.yml down")

    return True


if __name__ == "__main__":
    if len(sys.argv) > 1:
        setup_environment(sys.argv[1])
    else:
        print("Usage: python setup.py <pipeline_directory>")