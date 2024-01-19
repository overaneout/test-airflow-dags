from airflow.operators.python import is_venv_installed, PythonVirtualenvOperator
from airflow.models.dag import DAG
from datetime import datetime, timedelta

from airflow.models import Variable



haic_refresh_token = Variable.get("HAIC_REFRESH_TOKEN")
haic_domain = Variable.get("HAIC_DOMAIN")

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
    def test_virtualenv_call():
        """
        a dummy function to pip install files into python-venv
        
        """
        import h2o_mlops as mlops
        import driverlessai as dai

        print(mlops.__version__)
        print(dai.__version__)
        print("done")
    
    virtualenv_task = PythonVirtualenvOperator(task_id="test-virtualenv-python",python_callable=test_virtualenv_call,requirements=["driverlessai==1.10.6.2","h2o-mlops==0.62.1a5"], system_site_packages=False)
