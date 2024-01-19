from airflow.operators.python import is_venv_installed, PythonVirtualenvOperator
from airflow.models.dag import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import task



haic_refresh_token = Variable.get("HAIC_REFRESH_TOKEN")
haic_domain = Variable.get("HAIC_DOMAIN")
mlops_task_reqs = ["h2o-mlops==0.62.1a5","h2o_authn==1.1.1"]
# dai_task_reqs = ["driverlessai==1.10.6.2"]

with DAG(
    "model-retraining-test-1",
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

    @task.virtualenv(task_id="test-virtualenv-python",requirements=mlops_task_reqs, system_site_packages=False)
    def test_virtualenv_call():
        """
        a dummy function to pip install files into python-venv
        
        """
        import h2o_mlops as mlops
        import h2o_authn as authn
        token_provider = authn.TokenProvider(
            refresh_token=haic_refresh_token,
            client_id="hac-platform-public", 
            token_endpoint_url=f"http://auth.{haic_domain}/auth/realms/hac/protocol/openid-connect/token" 
            )
        mlops_client = mlops.Client(
            gateway_url=f"http://mlops-api.{haic_domain}",
            token_provider=token_provider
        )
        print("mlops projects")
        print(mlops_client.projects.list())
    
    virtualenv_task = test_virtualenv_call
