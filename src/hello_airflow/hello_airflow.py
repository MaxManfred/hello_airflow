import datetime
from typing import Dict, Any

from airflow import DAG
from airflow.models.abstractoperator import DEFAULT_PRIORITY_WEIGHT, DEFAULT_WEIGHT_RULE, DEFAULT_QUEUE, \
    DEFAULT_POOL_SLOTS, DEFAULT_TASK_EXECUTION_TIMEOUT, DEFAULT_TRIGGER_RULE
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator, DataprocCreateClusterOperator, \
    DataprocDeleteClusterOperator, DataprocSubmitJobOperator

# utility functions

project_id = 'training-386309'
region = 'europe-west8'
zone = 'europe-west8-a'
bucket_name = 'training-386309'
cluster_name = 'training-cluster'
pyspark_job_runner_file_name = 'runner.py'
parquet_file_name = 'people.parquet'


def _create_default_args() -> Dict[str, Any]:
    # If you are running Airflow in more than one time zone
    # see https://airflow.apache.org/docs/apache-airflow/stable/timezone.html
    # for best practices
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

    # see https://airflow.apache.org/docs/apache-airflow/2.5.1/_api/airflow/models/baseoperator/index.html#airflow.models.baseoperator.BaseOperator
    return {
        # owner (str) – the owner of the task. Using a meaningful description (e.g. user/person/team/role name) to clarify
        # ownership is recommended.
        'owner': 'Hello Airflow Example',
        # email (str | Iterable[str] | None) – the ‘to’ email address(es) used in email alerts.
        # This can be a single email or multiple ones.
        # Multiple addresses can be specified as a comma or semicolon separated string or by passing a list of strings.
        'email': [''],
        # email_on_retry (bool) – Indicates whether email alerts should be sent when a task is retried
        'email_on_retry': False,
        # email_on_failure (bool) – Indicates whether email alerts should be sent when a task failed
        'email_on_failure': False,
        # retries (int | None) – the number of retries that should be performed before failing the task
        'retries': 1,
        # retry_delay (timedelta | float) – delay between retries, can be set as timedelta or float seconds, which will be
        # converted into timedelta, the default is timedelta(seconds=300).
        'retry_delay': datetime.timedelta(minutes=5),
        # retry_exponential_backoff (bool) – allow progressively longer waits between retries by using exponential backoff
        # algorithm on retry delay (delay will be converted into seconds)
        'retry_exponential_backoff': True,
        # max_retry_delay (timedelta | float | None) – maximum delay interval between retries, can be set as timedelta or
        # float seconds, which will be converted into timedelta.
        'max_retry_delay': datetime.timedelta(minutes=10),
        # start_date (datetime | None) – The start_date for the task, determines the execution_date for the first task instance.
        # The best practice is to have the start_date rounded to your DAG’s schedule_interval.
        # Daily jobs have their start_date some day at 00:00:00, hourly jobs have their start_date at 00:00 of a specific hour.
        # Note that Airflow simply looks at the latest execution_date and adds the schedule_interval to determine the
        # next execution_date.
        'start_date': yesterday,
        # end_date (datetime | None) – if specified, the scheduler won’t go beyond this date
        # 'end_date': TODO,
        # depends_on_past (bool) – when set to true, task instances will run sequentially and only if the previous instance
        # has succeeded or has been skipped. The task instance for the start_date is allowed to run.
        'depends_on_past': False,
        # wait_for_downstream (bool) – when set to true, an instance of task X will wait for tasks immediately downstream
        # of the previous instance of task X to finish successfully or be skipped before it runs.
        # This is useful if the different instances of a task X alter the same asset, and this asset is used by tasks
        # downstream of task X. Note that depends_on_past is forced to True wherever wait_for_downstream is used.
        # Also note that only tasks immediately downstream of the previous task instance are waited for; the statuses
        # of any tasks further downstream are ignored.
        'wait_for_downstream': False,
        # dag (DAG | None) – a reference to the dag the task is attached to (if any)
        # 'dag': None,
        # priority_weight (int) – priority weight of this task against other task. This allows the executor to trigger
        # higher priority tasks before others when things get backed up. Set priority_weight as a higher number for more important tasks.
        'priority_weight': DEFAULT_PRIORITY_WEIGHT,
        # weight_rule (str) – weighting method used for the effective total priority weight of the task.
        # Options are: { downstream | upstream | absolute } default is downstream When set to downstream the effective
        # weight of the task is the aggregate sum of all downstream descendants. As a result, upstream tasks will have
        # higher weight and will be scheduled more aggressively when using positive weight values. This is useful when
        # you have multiple dag run instances and desire to have all upstream tasks to complete for all runs before each
        # dag can continue processing downstream tasks. When set to upstream the effective weight is the aggregate sum
        # of all upstream ancestors. This is the opposite where downstream tasks have higher weight and will be scheduled
        # more aggressively when using positive weight values. This is useful when you have multiple dag run instances
        # and prefer to have each dag complete before starting upstream tasks of other dags. When set to absolute, the
        # effective weight is the exact priority_weight specified without additional weighting. You may want to do this
        # when you know exactly what priority weight each task should have. Additionally, when set to absolute, there is
        # bonus effect of significantly speeding up the task creation process as for very large DAGs. Options can be set
        # as string or using the constants defined in the static class airflow.utils.WeightRule
        'weight_rule': DEFAULT_WEIGHT_RULE,
        # queue (str) – which queue to target when running this job. Not all executors implement queue management,
        # the CeleryExecutor does support targeting specific queues.
        'queue': DEFAULT_QUEUE,
        # pool (str | None) – the slot pool this task should run in, slot pools are a way to limit concurrency for certain tasks
        'pool': None,
        # pool_slots (int) – the number of pool slots this task should use (>= 1) Values less than 1 are not allowed.
        'pool_slots': DEFAULT_POOL_SLOTS,
        # sla (timedelta | None) – time by which the job is expected to succeed. Note that this represents the timedelta
        # after the period is closed. For example if you set an SLA of 1 hour, the scheduler would send an email soon
        # after 1:00AM on the 2016-01-02 if the 2016-01-01 instance has not succeeded yet. The scheduler pays special
        # attention for jobs with an SLA and sends alert emails for SLA misses. SLA misses are also recorded in the
        # database for future reference. All tasks that share the same SLA time get bundled in a single email, sent
        # soon after that time. SLA notification are sent once and only once for each task instance.
        'sla': None,
        # execution_timeout (timedelta | None) – max time allowed for the execution of this task instance, if it goes beyond
        # it will raise and fail.
        'execution_timeout': DEFAULT_TASK_EXECUTION_TIMEOUT,
        # on_failure_callback (TaskStateChangeCallback | None) – a function to be called when a task instance of this task fails.
        # a context dictionary is passed as a single parameter to this function. Context contains references to related
        # objects to the task instance and is documented under the macros section of the API.
        'on_failure_callback': None,
        # on_execute_callback (TaskStateChangeCallback | None) – much like the on_failure_callback except that it is
        # executed right before the task is executed.
        'on_execute_callback': None,
        # on_retry_callback (TaskStateChangeCallback | None) – much like the on_failure_callback except that it is executed
        # when retries occur.
        'on_retry_callback': None,
        # on_success_callback (TaskStateChangeCallback | None) – much like the on_failure_callback except that it is
        # executed when the task succeeds.
        'on_success_callback': None,
        # trigger_rule (str) – defines the rule by which dependencies are applied for the task to get triggered.
        # Options are: { all_success | all_failed | all_done | all_skipped | one_success | one_done | one_failed |
        # none_failed | none_failed_min_one_success | none_skipped | always} default is all_success.
        # Options can be set as string or using the constants defined in the static class airflow.utils.TriggerRule
        'trigger_rule': DEFAULT_TRIGGER_RULE,
        # resources (dict[str, Any] | None) – A map of resource parameter names (the argument names of the Resources constructor) to their values.
        'resources': None,
        # run_as_user (str | None) – unix username to impersonate while running the task
        'run_as_user': None,
        # max_active_tis_per_dag (int | None) – When set, a task will be able to limit the concurrent runs across execution_dates.
        'max_active_tis_per_dag': None,
        # executor_config (dict | None) – Additional task-level configuration parameters that are interpreted by a
        # specific executor. Parameters are namespaced by the name of executor.
        # Example:
        #     MyOperator(...,
        #         executor_config={
        #             "KubernetesExecutor":
        #             {"image": "myCustomDockerImage"}
        #         }
        #     )
        'executor_config': None,
        # do_xcom_push (bool) – if True, an XCom is pushed containing the Operator’s result
        'do_xcom_push': True,
        # task_group (TaskGroup | None) – The TaskGroup to which the task should belong. This is typically provided when
        # not using a TaskGroup as a context manager.
        'task_group': None,
        # doc (str | None) – Add documentation or notes to your Task objects that is visible in Task Instance details View in the Webserver
        'doc': None,
        # doc_md (str | None) – Add documentation (in Markdown format) or notes to your Task objects that is visible in Task Instance details View in the Webserver
        'doc_md': None,
        # doc_rst (str | None) – Add documentation (in RST format) or notes to your Task objects that is visible in Task Instance details View in the Webserver
        'doc_rst': None,
        # doc_json (str | None) – Add documentation (in JSON format) or notes to your Task objects that is visible in Task Instance details View in the Webserver
        'doc_json': None,
        # doc_yaml (str | None) – Add documentation (in YAML format) or notes to your Task objects that is visible in Task Instance details View in the Webserver
        'doc_yaml': None
    }


def _create_start_dataproc_cluster_operator() -> DataprocCreateClusterOperator:
    # create a Dataproc cluster configuration
    cluster_configuration = ClusterGenerator(
        cluster_name=cluster_name,
        project_id=project_id,
        num_workers=2,
        storage_bucket=bucket_name,
        init_actions_uris=[f'gs://{bucket_name}/init_actions.sh'],
        master_machine_type='e2-standard-2',
        master_disk_size=500,
        worker_machine_type='e2-standard-2',
        worker_disk_size=500,
        image_version='2.1-ubuntu20',
        autoscaling_policy=f'projects/{project_id}/locations/{region}/autoscalingPolicies/training-autoscaling-policy',
        optional_components=['JUPYTER'],
        zone=zone,
        region=region,
        enable_component_gateway=True
    ).make()

    # create a Dataproc cluster using the just created configuration
    operator = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=project_id,
        cluster_config=cluster_configuration,
        region=region,
        cluster_name=cluster_name,
    )

    return operator


def _create_pyspark_job_submit_operator():
    # create a pyspark specific configuration
    job_configuration = {
        'reference': {'project_id': project_id},
        'placement': {'cluster_name': cluster_name},
        'pyspark_job': {
            'main_python_file_uri': f'gs://{bucket_name}/{pyspark_job_runner_file_name}',
            'args': [f'runner=gs://{bucket_name}/{parquet_file_name}', 'temporary_view=people', 'gender=M']
        }
    }

    # create pyspark submit job operator
    operator = DataprocSubmitJobOperator(
        task_id='execute_pyspark_job',
        job=job_configuration,
        region=region,
        project_id=project_id
    )

    return operator


def _create_delete_dataproc_cluster_operator() -> DataprocDeleteClusterOperator:
    operator = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=project_id,
        cluster_name=cluster_name,
        region=region,
    )

    return operator


# declare the DAG

# @dag(
#     dag_id='hello_airflow',
#     catchup=False,
#     default_args=_create_default_args(),
#     schedule_interval=datetime.timedelta(days=1)
# )
# def create_hello_airflow_dag():
#     @task()
#     def print_dag_run_id():
#         print_dag_run_conf = BashOperator(task_id='print_dag_run_conf', bash_command='echo {{ dag_run.id }}')
#         return print_dag_run_conf
#
#     # create DAG arcs
#     print_dag_run_id()
#
#     # sample code to show how to create connections between tasks
#     # order_data = extract()
#     # order_summary = transform(order_data)
#     # load(order_summary["total_order_value"])
#
#
# # execute the DAG
# create_hello_airflow_dag()

with DAG(
        'hello_dataproc',
        catchup=False,
        default_args=_create_default_args(),
        schedule_interval=datetime.timedelta(days=1)
) as dag:
    # see https://blog.searce.com/running-spark-on-cloud-dataproc-and-loading-results-to-bigquery-using-apache-airflow-7d3722b302ce
    create_dataproc_cluster_operator = _create_start_dataproc_cluster_operator()

    # print the dag_run id from the Airflow logs
    # print_dag_run_conf = BashOperator(task_id='print_dag_run_conf', bash_command='echo {{ dag_run.id }}')

    submit_pyspark_job_operator = _create_pyspark_job_submit_operator()

    delete_dataproc_cluster_operator = _create_delete_dataproc_cluster_operator()

    # create_dataproc_cluster_operator >> [print_dag_run_conf, submit_pyspark_job_operator] >> delete_dataproc_cluster_operator

    create_dataproc_cluster_operator >> submit_pyspark_job_operator >> delete_dataproc_cluster_operator
