from airflow.operators.python import is_venv_installed, PythonVirtualenvOperator
from airflow.models.dag import DAG
from datetime import datetime, timedelta
from airflow.models import Variable



haic_refresh_token = Variable.get("HAIC_REFRESH_TOKEN")
haic_domain = Variable.get("HAIC_DOMAIN")
deployment_id = Variable.get("TESTING_DEPLOYMENT_ID")
mlops_task_reqs = ["h2o-mlops==0.62.1a5","h2o_authn==1.1.1","pytz==2023.3"]
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

    def test_virtualenv_call(haic_refresh_token,haic_domain,deployment_id):
        """
        a dummy function to pip install files into python-venv
        
        """
        import h2o_mlops_client as mlops ## Legacy ui to do monitoring
        import h2o_authn as authn
        import pytz
        # print(haic_refresh_token)
        # print(haic_domain)
        # print(deployment_id)

        token_provider = authn.TokenProvider(
            refresh_token=haic_refresh_token,
            client_id="hac-platform-public",
            token_endpoint_url=f"http://auth.{haic_domain}/auth/realms/hac/protocol/openid-connect/token" 
            )
        mlops_client = mlops.Client(
            gateway_url=f"http://mlops-api.{haic_domain}",
            token_provider=token_provider
        )
        end_time = datetime.datetime.now(pytz.utc)
        start_time = end_time - datetime.timedelta(days=30)
        drift_metrics = mlops_client._backend._model_monitoring._model_monitoring_service.get_model_drift_metrics(deployment_id=deployment_id,
                                                                                                  start_date_time=start_time,
                                                                                                  end_date_time=end_time,
                                                                                                  model_id=deployment_id) 
        print(drift_metrics)


    
    virtualenv_task = PythonVirtualenvOperator(task_id="test-mlops-connection",
                                               python_callable=test_virtualenv_call,
                                               requirements=mlops_task_reqs, 
                                               system_site_packages=False,
                                               op_args=[haic_refresh_token,haic_domain,deployment_id]
                                               )
    
