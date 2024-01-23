from airflow.operators.python import PythonVirtualenvOperator
from airflow.models.dag import DAG
from datetime import datetime, timedelta
from airflow.models import Variable



endpoint_url = Variable.get("HAIC_CC_MLOPS_ENDPOINT")
checking_task_reqs = ["h2o_mlops_scoring_client==0.1.4b1"]
scoring_task_reqs = ["h2o_mlops_scoring_client==0.1.4b1","s3fs~=2022.5.0"]


# minio_endpoint = "minio.minio.svc.cluster.local:9000"
# minio_bucket = "mlops-demo"
# minio_access_key = "mlops-test-key"
# minio_access_secret_key = Variable.get(minio_access_key)
# minio_creds = (minio_access_key,minio_access_secret_key)
# data_source = "credit_card_test.csv"
# data_sink = "credit_card_output.csv"
data_info = {
    "minio_endpoint": "minio.minio.svc.cluster.local:9000",
    "minio_access_key": "mlops-test-key",
    "minio_access_secret_key": Variable.get("mlops-test-key"),
    "data_source": "s3://mlops-demo/mlops-source/credit_card_test.csv",
    "data_sink": "s3://mlops-demo/mlops-source/credit_card_output.csv"
}


with DAG(
    "model-scoring-lightweight",
    default_args = {
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="testing dag",
    schedule = None,         
    start_date=datetime(2024,1,17),
    catchup=False,
) as dag:


    def model_endpoint_check(endpoint_url):
        """
        check mlops endpoint using get schema
        if get schema fails, means endpoint has issues/not healthy
        """
        import h2o_mlops_scoring_client
        get_schema = h2o_mlops_scoring_client.get_schema(endpoint_url)
        print(get_schema)
        


    def model_scoring(endpoint_url,data_info):
        """
        do model scoring
        """
        import h2o_mlops_scoring_client
        import pandas as pd
        data_to_score = pd.read_csv(data_info.get("data_source"),
                                    storage_options={
                                        "key": data_info.get("minio_access_key"),
                                        "secret": data_info.get("minio_access_secret_key"),
                                        "client_kwargs":{
                                            endpoint_url: data_info.get("minio_endpoint")                                            
                                        }
                                    })
        print(data_to_score)

        
    model_endpoint_check_task = PythonVirtualenvOperator(task_id="endpoint_check",
                                               python_callable=model_endpoint_check,
                                               requirements=scoring_task_reqs, 
                                               system_site_packages=False,
                                               op_args=[endpoint_url]
                                               )

    model_scoring_task = PythonVirtualenvOperator(task_id="model_scoring",
                                               python_callable=model_scoring,
                                               requirements=scoring_task_reqs, 
                                               system_site_packages=False,
                                               op_args=[endpoint_url,data_info]
                                               )
 
    model_endpoint_check_task >> model_scoring_task