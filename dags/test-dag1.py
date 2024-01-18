from airflow.decorators import task
from airflow.operators.python import is_venv_installed
from airflow.models.dag import DAG
from datetime import datetime, timedelta

@task.virtualenv(task_id="test-virtualenv-python",requirements=["driverlessai==1.10.6.2","h2o-mlops==0.62.1a5"], system_site_packages=False)
def test_virtualenv_call():
    """
    a dummy function to pip install files into python-venv
       
    """
    import h2o_mlops as mlops
    import driverlessai as dai

    print(mlops.__version__)
    print(dai.__version__)

    print("done")


with DAG(
    "testing",
    default_args = {
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="testing dag",
    schedule = "*/5 * * * *",         
    start_date=datetime(2024,1,17),
    catchup=False,
) as dag:
    virtualenv_task = test_virtualenv_call()

