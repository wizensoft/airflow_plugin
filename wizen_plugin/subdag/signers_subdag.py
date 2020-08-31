import logging
from airflow.models import DAG
from airflow.models import Variable
from airflow.models import TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
# 작업
INSTANCE_TASK = 'instances_task'
# 상태 정보
STATUS_00 = '00' # 기안
STATUS_01 = '01' # 진행중 : 결재
STATUS_02 = '02' # 완료
STATUS_03 = '03'
STATUS_04 = '04'
STATUS_05 = '05'

def signers_subdag(parent_dag_name, child_dag_name, args, parent_dag):
    #instances = context['ti'].xcom_pull(task_ids=INSTANCE_TASK, key=INSTANCE_TASK)
    subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name), # subdag의 id는 이와같은 컨벤션으로 쓴답니다.
        default_args=args,
        schedule_interval=None, # 값을 넣어주어야 합니다.
    )
    def values_function(**context):
        instances = context['ti'].xcom_pull(dag_id=parent_dag_name,task_ids=INSTANCE_TASK)
        return instances
    
    # test_list = parent_dag.get_task_instances(settings.Session, start_date=parent_dag.get_active_runs()[-1])[-1].xcom_pull(
    # dag_id='%s.%s' % (parent_dag_name, 'signers_subdag'),
    # task_ids=INSTANCE_TASK)

        # logging.info(f'args: {args}')
        # if '00' == STATUS_00:
        #     return STATUS_00
        # else:
        #     return STATUS_01
    # logging.info(f'parent_dag_name: {parent_dag_name}')
    # logging.info(f'child_dag_name: {child_dag_name}')
    # instances = context['ti'].xcom_pull(dag_id=parent_dag_name,task_ids=INSTANCE_TASK, key=INSTANCE_TASK)
    # logging.info(f'instances: {test_list}')
    # logging.info(f'args: {main}')  
    # for row in values_function():
    #     logging.info(f'row: {row}')

    # globals()['process_status'] = BranchPythonOperator(
    #     task_id='%s_status_task_%s' % (child_dag_name, 1),
    #     default_args=args,
    #     python_callable=branch_status, 
    #     # op_kwargs={'number_of_runs': number_of_runs},
    #     provide_context=True, 
    #     dag=subdag
    # )

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

    # process_status

    return subdag