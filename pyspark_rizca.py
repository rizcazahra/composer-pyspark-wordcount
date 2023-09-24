# Import necessary modules and classes from Airflow
import datetime
import os
from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

# Define the output directory path for the Cloud Dataproc job result
output_dir = (
    os.path.join(
        "gs://us-east1-pysparkproject-c41e01d2-bucket/output",
        "wordcount",
        datetime.datetime.now().strftime("%Y%m%d-%H%M%S"),
    )
    + os.sep
)

# Define the path to the PySpark script that will perform word count
pyspark_script = "gs://us-east1-pysparkproject-c41e01d2-bucket/dags/pyspark_rizca.py"

# Define the arguments to pass to the PySpark job
wordcount_args = ["spark-submit", pyspark_script, "gs://pub/shakespeare/rose.txt", output_dir]

# Define the 'yesterday' timestamp for scheduling purposes
yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time()
)

# Define default DAG arguments
default_dag_args = {
    "start_date": yesterday,  # Start the DAG from yesterday's date
    "email_on_failure": False,  # Do not send emails on failure
    "email_on_retry": False,  # Do not send emails on retry
    "retries": 1,  # Retry a task once after waiting at least 5 minutes
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": "yspark-project-400006",  # Your Google Cloud Project ID
}

# Create a DAG named "composer_pyspark_wordcount" with the specified default arguments
with models.DAG(
    "composer_pyspark_wordcount",
    schedule_interval=datetime.timedelta(days=1),  # Run the DAG daily
    default_args=default_dag_args,
) as dag:

    # Task 1: Create a Cloud Dataproc cluster
    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id="create_dataproc_cluster",
        cluster_name="composer-pyspark-wordcount-cluster-{{ ds_nodash }}",  # Generate a unique cluster name
        num_workers=2,  # Number of worker nodes
        project_id="yspark-project-400006",  # Your Google Cloud Project ID
        region="us-east1",  # Google Compute Engine region
        zone=models.Variable.get('gce_zone'),  # Get the GCE zone from Airflow Variables
        master_machine_type="e2-standard-2",  # Master node machine type
        worker_machine_type="e2-standard-2",  # Worker node machine type
        image_version="2.0",  # Dataproc image version
    )

    # Task 2: Run the PySpark word count job on the Cloud Dataproc cluster
    run_dataproc_pyspark = dataproc_operator.DataProcPySparkOperator(
        task_id="run_dataproc_pyspark",
        main=pyspark_script,  # Path to the PySpark script
        region="us-east1",  # Google Compute Engine region
        cluster_name="composer-pyspark-wordcount-cluster-{{ ds_nodash }}",  # Cluster name
        arguments=wordcount_args,  # Arguments for the PySpark job
    )

    # Task 3: Delete the Cloud Dataproc cluster
    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id="delete_dataproc_cluster",
        cluster_name="composer-pyspark-wordcount-cluster-{{ ds_nodash }}",  # Cluster name
        region="us-east1",  # Google Compute Engine region
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE,  # Delete the cluster after all tasks are done
    )

    # Define DAG dependencies
    create_dataproc_cluster >> run_dataproc_pyspark >> delete_dataproc_cluster
