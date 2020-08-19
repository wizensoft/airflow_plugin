import logging
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
# 작업
INSTANCE_TASK = 'instances_task'
# 상태 정보
STATUS_00 = '00' # 기안
STATUS_01 = '01' # 진행중 : 결재
STATUS_02 = '02' # 완료
STATUS_03 = '03'
STATUS_04 = '04'
STATUS_05 = '05'

def branch_status(instance, **kwargs):
    if instance["state"] == STATUS_00:
        return STATUS_00
    else:
        return STATUS_01

def signers_subdag(parent_dag_name, child_dag_name, instances, args):
    #instances = context['ti'].xcom_pull(task_ids=INSTANCE_TASK, key=INSTANCE_TASK)
    subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name), # subdag의 id는 이와같은 컨벤션으로 쓴답니다.
        default_args=args,
        schedule_interval=None, # 값을 넣어주어야 합니다.
    )
    logging.info(f'parent_dag_name: {parent_dag_name}')
    logging.info(f'child_dag_name: {child_dag_name}')
    logging.info(f'instances: {instances}')
    logging.info(f'args: {args}')

    # for row in instances:
    #     instance_id = str(row[0])
    #     globals()['process_status'] = PythonOperator(
    #         task_id='%s-status_task-%s' % (child_dag_name, instance_id),
    #         default_args=args,
    #         python_callable=branch_status, 
    #         op_kwargs=row,
    #         provide_context=True, 
    #         dag=subdag
    #     )
        # # 00: 기안
        # status_00 = PythonOperator(task_id='status_00_task',python_callable=get_status_00, provide_context=True, dag=dag)
        # # 01: 현결재 true || false
        # status_01 = BashOperator(task_id='status_01_task',bash_command='echo get status 현결재 true || false',dag=dag)


    return subdag