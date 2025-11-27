from airflow.sdk import DAG
import pendulum
import datetime

# AWS Hook 및 Python Operator 임포트
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.standard.operators.python import PythonOperator

# ==========================================================
# 1. 💡 PythonOperator가 호출할 함수를 정의합니다.
# ==========================================================
# 함수 정의 시 Connection ID 기본값을 'dags_s3_ai'로 지정합니다.
def s3_download_callable(aws_conn_id: str = 'dags_s3_ai'):
    """S3에서 CSV 파일을 로드하는 함수. 인증은 Airflow Connection을 사용합니다."""
    import pandas as pd

    s3_path = "s3://human09-2474/HDD/model_2017_ST4000DM000.csv"

    # Airflow Connection에서 인증 정보 가져오기
    # 💡 aws_conn_id에 'dags_s3_ai' 사용
    hook = AwsBaseHook(aws_conn_id, client_type='s3')
    creds = hook.get_credentials()

    df = pd.read_csv(
        s3_path,
        storage_options={
            # 💡 Connection에서 가져온 키 사용
            "key": creds.access_key,
            "secret": creds.secret_key,
            "client_kwargs": {"region_name": "ap-northeast-2"}
        }
    )

    print(f"S3 파일 로드 완료. 데이터프레임 크기: {len(df)}")
    print(f"S3 파일 로드 완료. Head: {df.head()}")

    return True

# ==========================================================
# 2. 📁 DAG 정의 (Airflow 3.x 예상)
# ==========================================================
with DAG(
    # DAG ID는 그대로 유지
    dag_id="dags_s3_pipline_01",
    start_date=pendulum.datetime(2025, 11, 27, tz="Asia/Seoul"),
    catchup=False
) as dag:

    s3_download = PythonOperator(
        task_id='s3_download',
        python_callable=s3_download_callable,
        # 💡 op_kwargs를 통해 Connection ID 명시적으로 전달
        op_kwargs={'aws_conn_id': 'dags_s3_ai'}
    )
