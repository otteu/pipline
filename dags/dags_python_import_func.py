# 기존: from airflow.models.dag import DAG
# 변경: Airflow 3.0 부터는 DAG 클래스를 airflow.sdk에서 임포트합니다.
from airflow.sdk import DAG 
import pendulum
import datetime

# 기존: from airflow.operators.python import PythonOperator
# 변경: Airflow 3.0 부터는 Provider 경로를 통해 임포트합니다.
from airflow.providers.standard.operators.python import PythonOperator 
# 참고: PythonOperator의 provider는 실제로 'standard'에서 'cncf.kubernetes'로 이동하지 않았습니다. 
# Airflow 3.x는 아직 정식 릴리스되지 않았으며, 공식 문서상 PythonOperator는 'standard' provider에 
# 포함될 가능성이 높습니다.
# 다만, 사용자 정의 Operator가 아닌 '기본' Operator라도 provider 경로를 사용하는 추세에 따라
# 현재 Airflow 3.x 개발 버전에서 일반적으로 사용하는 예상 경로를 적용했습니다.

from common.common_fun import get_sftp

with DAG(
    dag_id="dags_python_import_func",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    task_get_sftp = PythonOperator(
        task_id='task_get_sftp',
        python_callable=get_sftp,
    )