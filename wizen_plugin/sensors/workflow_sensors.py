import logging
from datetime import datetime
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.hooks.mysql_hook import MySqlHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator

WORKFLOW_PROCESS = 'workflow_process'
class WorkflowSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(WorkflowSensor, self).__init__(*args, **kwargs)
          
    def poke(self, context):
        db = MySqlHook(mysql_conn_id='mariadb', schema="djob")
        
        sql = """
        select
            workflow_process_id,ngen,site_id,application_id,instance_id,schema_id,name,workflow_instance_id,state,retry_count,ready,
            execute_date,created_date,bookmark,version,request,reserved,message
        from
            workflow_process
        where 
            ready > 0 and retry_count < 10
        """
        tasks = {}
        tasks[WORKFLOW_PROCESS] = []
        rows = db.get_records(sql)
        for row in rows:
            model = {
                'workflow_process_id':row[0],
                'ngen':row[1],
                'site_id':row[2],
                'application_id':row[3],
                'instance_id':row[4],
                'schema_id':row[5],
                'name':row[6],
                'workflow_instance_id':row[7],
                'state':row[8],
                'retry_count':row[9],
                'ready':row[10],
                'execute_date':str(row[11]),
                'created_date':str(row[12]),
                'bookmark':row[13],
                'version':row[14],
                'request':row[15],
                'reserved':row[16],
                'message':''
            }
            tasks[WORKFLOW_PROCESS].append(model)
            sql = f"""
            update workflow_process
                set ready = 0, bookmark = 'start'
            where workflow_process_id = %s
            """
            db.run(sql, autocommit=True, parameters=[str(row[0])])

        # 객체가 있는 경우 처리
        if tasks[WORKFLOW_PROCESS]:
            logging.info('workflow_process find new data')
            context['ti'].xcom_push(key=WORKFLOW_PROCESS, value=tasks[WORKFLOW_PROCESS])
            return True
        else:
            logging.info('workflow_process empty data')
            return False

        # current_minute = datetime.utcnow().minute
        # if current_minute % 3 != 0:
        #     log.info("Current minute (%s) not is divisible by 3, sensor will retry.", current_minute)
        #     return False
            
        # log.info("Current minute (%s) is divisible by 3, sensor finishing.", current_minute)
        # return True