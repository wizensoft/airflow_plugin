import sys
import json
import logging
from airflow import DAG
from airflow import models
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator

BOOKMARK = 'signers'
SIGNER_TASK = 'signers'
SIGNER_TASK_ING = 'signers_ing'
SIGNER_TASK_COMPLETED = 'signers_completed'
# 결재선
SIGNERS = 'signers'
SIGN_USERS = 'sign_users'
SIGN_AREAS = 'sign_areas'
SIGN_ACTIVITY = 'sign_activity'
SIGN_AREA = 'sign_area'
SIGN_SECTION = 'sign_section'
SIGN_POSITION = 'sign_position'
SIGN_ACTION = 'sign_action'
# 결재선 필드
SEQ = 'sequence'
BOX_ID = 'box_id'
NAME = 'name'
CULTURE = 'culture'
USER_ID = 'user_id'
USER_NAME = 'user_name'
INSTANCE_ID = 'instance_id'
GROUP_ID = 'group_id'
GROUP_NAME = 'group_name'
INSTANCE_ID = 'instance_id'
SIGN_AREA_ID = 'sign_area_id'
IS_EXECUTED = 'is_executed'
IS_DISPLAY = 'is_display'
IS_COMMENT = 'is_comment'
LOCATION = 'location'
DELAY_TIME = 'delay_time'
CREATED_DATE ='created_date'
HOST_ADDRESS = 'host_address'
# 상태 정보
STATUS_00 = '00' # 기안
STATUS_01 = '01' # 진행중 : 결재
STATUS_02 = '02' # 완료
STATUS_03 = '03' # 반려
STATUS_04 = '04'
STATUS_05 = '05'
# 결재함
BOXES = 'boxes'
BOX_04 = 4   # 개인 결재함
BOX_09 = 9   # 개인 반려함
BOX_10 = 10  # 개인 완료함
BOX_13 = 13  # 부서 수신함
BOX_15 = 15  # 품의 완료함
BOX_16 = 16  # 발신 완료함
BOX_17 = 17  # 수신 완료함
# 결재 위치 정보
LOCATION_AGREE = 'agree'    # 합의
LOCATION_COOP = 'coop'      # 협조
LOCATION_DRAFT = 'draft'    # 품의
LOCATION_RCV = 'rcv'        # 수신
LOCATION_REQ = 'req'        # 요청
LOCATION_SEND = 'send'      # 발신
# 결재 예정 정보
CURRENT_ACTIVITY = 'current_activity'
NEXT_ACTIVITY = 'next_activity'  

class SignersOperator(BaseOperator):        
    """
    결재선 & 결재함
    """
    @apply_defaults
    def __init__(self, wf_start_task, *args, **kwargs):

        super(SignersOperator, self).__init__(*args, **kwargs)
        self.wf_start_task = wf_start_task

    def execute(self, context):
        if self.wf_start_task == 'receive':
            if context["dag_run"]:
                lst = context["dag_run"].conf["workflows"]
                # logging.info(f'lst: {lst}')
                if lst:  
                    workflows = json.loads(lst.replace("'","\""))
            else:
                logging.info(f"airflow test workflow_trigger_signers signers '2020-09-02'")
                data = "[{'workflow_process_id': 25, 'ngen': 0, 'site_id': 1, 'application_id': 1, 'instance_id': 1, 'schema_id': 0, 'name': '기안지', 'workflow_instance_id': '1', 'state': '00', 'retry_count': 0, 'ready': 1, 'execute_date': '2020-07-28 00:00:00', 'created_date': '2020-07-28 00:00:00', 'bookmark': '', 'version': '1.0', 'request': '', 'reserved': 0, 'message': ''}]"
                workflows = json.loads(data.replace("'","\""))
        else:
            workflows = context['ti'].xcom_pull(task_ids=self.wf_start_task)
        # instances = context['ti'].xcom_pull(task_ids=self.instance_task)
        # settings = context['ti'].xcom_pull(task_ids=self.setting_task)
        
        db = MySqlHook(mysql_conn_id='mariadb', schema="dapp")
        if workflows:
            tasks = {}
            tasks[SIGNER_TASK_ING] = []
            tasks[SIGNER_TASK_COMPLETED] = []
            for wf in workflows:
                try:
                    sql = []
                    workflow_process_id = wf['workflow_process_id']
                    instance_id = wf[INSTANCE_ID]

                    # 기 결재선
                    signers = get_signers(instance_id)
                    # 이전 결재자
                    prev_signers = []
                    if signers[0]:
                        index = 0
                        sign_area_id = 1
                        seq = 1
                        current = {}
                        for m in signers[0]:
                            # 첫 결재자 정보
                            if index == 0:
                                current = m
                            else:
                                # 같은 영역에서 최종 결재자 정보 확인
                                if current[SIGN_AREA_ID] == m[SIGN_AREA_ID]:
                                    if m[SEQ] > seq:
                                        seq = m[SEQ]
                                        sign_area_id = m[SIGN_AREA_ID]
                                else:
                                    # 영역이 바뀌면 초기화
                                    sign_area_id = m[SIGN_AREA_ID]
                                    seq = 1
                            if sign_area_id == m[SIGN_AREA_ID] and m[SEQ] == seq:
                                prev_signers.append(m)
                                prev_signers_time = datetime.strptime(prev_signers[0][CREATED_DATE],  '%Y-%m-%d %H:%M:%S')
                            index += 1
                        # logging.info(f'이전 결재자: {prev_signers}')

                    # activities = {}
                    # activities[CURRENT_ACTIVITY] = []
                    # 결재 예정 정보(현결재자, 다음결재자)
                    sign_activity = get_sign_activity(instance_id)
                    # 현결재자[0] 다음결재자[1] 처리
                    activities, u_reject = fn_next_activity(sign_activity)
                    # 현 결재자
                    if activities[0]:
                        # 반려시 처리
                        if u_reject:
                            logging.info(f'반려 프로세스 진행')
                            # 기 결재자 확인
                            sign_users = get_sign_users(instance_id)
                            sign_user_lst = sign_users[0]
                            
                            # 대기자 결재선으로 이동
                            act_lst = []
                            user_lst = []
                            for m in sign_activity[0]:
                                uid = m[USER_ID]
                                gid = m[GROUP_ID]

                                # 그룹 정보 설정
                                group = get_group(gid)
                                m[GROUP_NAME] = group[NAME]
                                m[CULTURE] = group[CULTURE]
                                # 사용자 정보 및 확인
                                if uid:
                                    user = get_user(uid)
                                    user[INSTANCE_ID] = instance_id
                                    # 추가할 사용자 확인
                                    m[USER_NAME] = user[NAME]
                                    sign_user_lst.append(m)
                                    # 사용자 프로필 정보
                                    user[SEQ] = m[SEQ]
                                    user[HOST_ADDRESS] = m[HOST_ADDRESS]
                                    user[INSTANCE_ID] = m[INSTANCE_ID]
                                    user[SIGN_AREA_ID] = m[SIGN_AREA_ID]
                                    user[IS_COMMENT] = m[IS_COMMENT]
                                    user[DELAY_TIME] = 0
                                    # 이전 결재자 정보 확인
                                    if prev_signers:
                                        delay_time = date_diff_in_seconds(datetime.utcnow(), prev_signers_time)
                                        user[DELAY_TIME] = delay_time
                                    user_lst.append(user)
                                                                        
                                 # 결재자 등록                                
                                act_lst.append(m)                            
                            if act_lst:
                                sql.append(insert_signers(act_lst))                                
                            # 결재자 프로필 등록
                            if user_lst:
                                sql.append(insert_sign_users(user_lst))
                            # 반려함 등록
                            if sign_user_lst:
                                lst = fn_post_box_lst(sign_user_lst, instance_id, BOX_09, 'U', LOCATION_DRAFT)
                                sql.append(insert_post_boxes(lst))
                            # 대기중인 결재자 모두 삭제
                            sql.append(delete_activity_all(instance_id))
                        else:
                            # 현 결재자 대상 확인
                            user_lst = []
                            for m in activities[0]:
                                uid = m[USER_ID]
                                gid = m[GROUP_ID]
                                # 사용자 정보 및 확인
                                if uid:
                                    user = get_user(uid)
                                    user[INSTANCE_ID] = instance_id
                                    m[USER_NAME] = user[NAME]
                                    # 사용자 프로필 정보
                                    user[SEQ] = m[SEQ]
                                    user[HOST_ADDRESS] = m[HOST_ADDRESS]
                                    user[INSTANCE_ID] = m[INSTANCE_ID]
                                    user[SIGN_AREA_ID] = m[SIGN_AREA_ID]                                    
                                    user[IS_COMMENT] = m[IS_COMMENT]
                                    user[DELAY_TIME] = 0
                                    # 이전 결재자 정보 확인                                    
                                    if prev_signers:
                                        delay_time = date_diff_in_seconds(datetime.utcnow(), prev_signers_time)
                                        user[DELAY_TIME] = delay_time
                                    user_lst.append(user)
                                group = get_group(gid)
                                m[GROUP_NAME] = group[NAME]
                                m[CULTURE] = group[CULTURE]
                                # 결재자 등록
                                sql.append(insert_signers(activities[0]))
                                # 결재자 프로필 등록
                                sql.append(delete_activity(m[INSTANCE_ID], m[SIGN_AREA], m[SEQ]))
                            if user_lst:
                                sql.append(insert_sign_users(user_lst))
                            # 현 결재 프로세스 실행 후 다음 결재자 프로세스 진행
                            # 다음 결재자
                            if activities[1]:
                                next_lst = fn_next_activity_format(activities[1])
                                sql.append(insert_post_office(next_lst))
                                sql.append(update_activity(next_lst))
                                # 진행중인 프로세스
                                # tasks[SIGNER_TASK_ING].append(str(instance_id))
                            else:
                                logging.info(f'완료 프로세스 진행')
                                # tasks[SIGNER_TASK_COMPLETED].append(str(instance_id))
                                # 완료함 사용확인
                                boxes = json.loads(Variable.get(BOXES).replace("'","\""))
                                # logging.info(f'boxes: {boxes}')
                                u_completed = False
                                for m in boxes:
                                    if m[BOX_ID] == BOX_10 and m[IS_DISPLAY] == True:
                                        u_completed = True
                                # 개인완료함
                                if u_completed:
                                    sign_users = get_sign_users(instance_id)
                                    lst = fn_post_box_lst(sign_users[0], instance_id, BOX_10, 'U', LOCATION_DRAFT)
                                    if lst:
                                        sql.append(insert_post_boxes(lst))

                                # 품의서 || 서브 프로세스 분류
                                areas = get_sign_areas(instance_id)
                                area_keys = []
                                is_sub_process = False
                                for m in areas[0]:
                                    if m[SIGN_AREA] not in area_keys:
                                        area_keys.append(m[SIGN_AREA])
                                        # 기안(일반)결재 외 다른 결재가 있다면 우선 필터
                                        if m[SIGN_AREA] != STATUS_00 and m[SIGN_AREA] != STATUS_01:
                                            is_sub_process = True
                                
                                # 품의완료함
                                if is_sub_process == False:
                                    lst = fn_post_box_lst(signers[0], instance_id, BOX_15, 'G', LOCATION_DRAFT)
                                    if lst:
                                        sql.append(insert_post_boxes(lst))
                                else:
                                    logging.info(f'서브 프로세스 완료처리')
                                # 발신완료함
                                # 수신완료함

                        sql.append(complete_workflow(workflow_process_id))

                        logging.info(f'sql: {"".join(sql)}')

                        conn = db.get_conn()
                        cursor = conn.cursor()
                        cursor.execute(''.join(sql))
                        cursor.close()
                        conn.commit()
                    else:
                        logging.info(f'현 결재자 if m[SIGN_ACTION] == STATUS_01 and m[IS_EXECUTED] == True 없음')

                except Exception as ex:
                    db = MySqlHook(mysql_conn_id='mariadb', schema="djob")
                    sql = f"""
                    update workflow_process
                        set ready=1, retry_count=retry_count + 1, message=%s, bookmark=%s
                    where workflow_process_id=%s
                    """
                    db.run(sql, autocommit=True, parameters=[ex, BOOKMARK, workflow_process_id])
                    logging.error(f'Message: {ex}')
                finally:
                    logging.info(f'finally 결재 프로세스 종료')
                    # if (conn.is_connected()):
                    #     cursor.close()
                    #     conn.close()

            # 결재 진행중인 프로세스
            context['ti'].xcom_push(key=SIGNER_TASK_ING, value=tasks[SIGNER_TASK_ING])
            # 결재 완료된 프로세스
            context['ti'].xcom_push(key=SIGNER_TASK_COMPLETED, value=tasks[SIGNER_TASK_COMPLETED])
        else:
            logging.info(f'실행 할 djob.workflow_process 없음')

# def current_activity_format(lst):
#     """
#     현 결재자 대상 정보
#     """

def fn_next_activity_format(lst):
    """
    다음 결재자 대상 정보
    """
    result = []
    for m in lst:
        instance_id = m[INSTANCE_ID]
        uid = m[USER_ID]
        gid = m[GROUP_ID]
        # 사용자 정보 및 확인
        if uid:
            user = get_user(uid)
            user[INSTANCE_ID] = instance_id
        group = get_group(gid)                            
        # 결재영역
        name = fn_sign_area_name(m[SIGN_AREA])
        if m[SIGN_AREA] == STATUS_00:
            logging.info(f'{name}: {m}')
        elif m[SIGN_AREA] == STATUS_01:
            logging.info(f'{name}: {m}')
            m['box_id'] = BOX_04
            m['participant_id'] = uid
            m['row_version'] = '0x'
            m['name'] = name
            m['participant_name'] = user['name']
            m['location'] = LOCATION_DRAFT
            result.append(m)                                
        elif m[SIGN_AREA] == STATUS_02:
            logging.info(f'{name}: {m}')

        elif m[SIGN_AREA] == STATUS_03:
            logging.info(f'{name}: {m}')

        elif m[SIGN_AREA] == STATUS_04:
            logging.info(f'{name}: {m}')

        elif m[SIGN_AREA] == STATUS_05:
            logging.info(f'{name}: {m}')
        else:
            logging.info(f'현재없는 결재영역: {m}')

    return result

def fn_next_activity(lst):
    """
    현결재자[0] 다음결재자[1] 정보
    """
    tasks = {}
    tasks[CURRENT_ACTIVITY] = []    
    tasks[NEXT_ACTIVITY] = []
    # 반려 확인
    u_reject = False
    for m in lst[0]:
        # 현결재자 & 실행 프로세스 확인
        if m[SIGN_ACTION] == STATUS_01 and m[IS_EXECUTED] == True:
            # local variable 'current' referenced before assignment
            # 기결재
            m[SIGN_ACTION] = STATUS_00
            current = m
            logging.info(f'현 결재자: {m}')
            tasks[CURRENT_ACTIVITY].append(m)
            if not u_reject:
                if m[SIGN_POSITION] == STATUS_03:
                    u_reject = True
        # 미결재자
        elif m[SIGN_ACTION] == STATUS_02:
            # 반려시 다음 결재 프로세스 실행 안함
            if not u_reject:            
                # 일반결재
                if m[SIGN_AREA] == STATUS_01:
                    # 다음 결재자의 결재영역이 같은가?
                    if current[SIGN_AREA] == m[SIGN_AREA]:
                        if current[SEQ] + 1 == m[SEQ]:
                            # 일반결재
                            if m[SIGN_SECTION] == STATUS_01:
                                tasks[NEXT_ACTIVITY].append(m)
                            # 병렬합의
                            elif m[SIGN_SECTION] == STATUS_02: 
                                tasks[NEXT_ACTIVITY].append(m)
                    else:
                        if current[SIGN_AREA_ID] + 1 == m[SIGN_AREA_ID]:
                            # 일반결재
                            if m[SIGN_SECTION] == STATUS_01: 
                                # 일반결재 and 첫번째 결재
                                if m[SIGN_POSITION] == STATUS_01 and m[SEQ] == 1: 
                                    tasks[NEXT_ACTIVITY].append(m)
                            # 병렬합의
                            elif m[SIGN_SECTION] == STATUS_02: 
                                tasks[NEXT_ACTIVITY].append(m)

                # elif m[SIGN_AREA] == STATUS_02: # 수신결재

    return list(tasks.values()), u_reject

def fn_post_box(t, instance_id, box_id, m):
    """
    결재 완료함 정보
    """
    participant_id = m[USER_ID] if t == 'U' else m[GROUP_ID]
    participant_name = m[USER_NAME] if t == 'U' else m[GROUP_NAME]
    return {
        'box_id': box_id,
        'participant_id': participant_id,
        'instance_id': instance_id,
        'row_version': '0x',
        'name': fn_sign_area_name(m[SIGN_AREA]),
        'participant_name': participant_name,
        'location': m['location']
    }

def fn_post_box_lst(lst, instance_id, box_id, t, location):
    """
    결재 완료함 대상 목록 - 중복키 제거 구문 포함
    """
    keys = []
    result = []
    id = USER_ID if t == 'U' else GROUP_ID
    for m in lst:
        if m[id] not in keys:
            m[LOCATION] = location
            keys.append(m[id])
            result.append(fn_post_box(t, instance_id, box_id, m))
    return result

def fn_sign_area_name(sign_area):
    """
    결재 영역 명
    """    
    if sign_area == STATUS_00:
        name = '기안결재'
    elif sign_area == STATUS_01:
        name = "일반결재"
    elif sign_area == STATUS_02:
        name = '수신결재'
    elif sign_area == STATUS_03:
        name = '부서합의'
    elif sign_area == STATUS_04:
        name = '개인합의'
    elif sign_area == STATUS_05:
        name = '협조결재'
    else:
        logging.info(f'현재 없는 결재 영역')
    return name

def get_sign_activity(instance_id):
    """
    결재 예정 정보
    """
    db = MySqlHook(mysql_conn_id='mariadb', schema="dapp")
    sql = []
    sql.append(f"""
    select
        instance_id, sign_area_id, sequence, sign_area, sign_section, sign_position, sign_action, is_comment, is_executed, group_id, user_id, host_address
    from
        dapp.sign_activity
    where 
        instance_id = %s
    """)
    tasks = {}
    tasks[SIGN_ACTIVITY] = []
    rows = db.get_records(''.join(sql), parameters=[instance_id])
    for row in rows:
        model = {
            INSTANCE_ID:row[0],
            'sign_area_id':row[1],
            'sequence':row[2],
            'sign_area':row[3],
            'sign_section':row[4],
            'sign_position':row[5],
            'sign_action':row[6],
            'is_comment':row[7],
            'is_executed':row[8],
            'group_id':row[9],
            'user_id':row[10],
            'host_address':row[11]
        }
        tasks[SIGN_ACTIVITY].append(model)

    # context['ti'].xcom_push(key=SIGN_ACTIVITY, value=tasks[SIGN_ACTIVITY])
    return list(tasks.values())

def get_user(user_id):
    """
    사용자 정보
    """
    db = MySqlHook(mysql_conn_id='mariadb', schema="dbo")
    sql = []
    sql.append(f"""
    select
        user_id, name, culture, group_id, employee_num, anonymous_name, email, theme_code, date_format_code, time_format_code, time_zone, row_count, language_code, 
        interface_id, phone, mobile, fax, icon, addsign_img, is_plural, is_notification, is_absence, is_deputy 
    from
        dbo.users
    where 
        user_id = %s
    """)
    task = {}
    rows = db.get_records(''.join(sql), parameters=[user_id])
    for row in rows:
        model = {
            'user_id':row[0],
            'name':row[1],
            'culture':row[2],
            'group_id':row[3],
            'employee_num':row[4],
            'anonymous_name':row[5],
            'email':row[6],
            'theme_code':row[7],
            'date_format_code': row[8],
            'time_format_code':row[9],
            'row_count':row[10],
            'language_code':row[11],
            'interface_id':row[12],
            'phone':row[13],
            'mobile':row[14],
            'fax':row[15],
            'icon':row[16],
            'addsign_img':row[17],
            'is_plural':row[18],
            'is_notification':row[19],
            'is_absence':row[20],
            'is_deputy':row[21]
        }
        task = model

    # context['ti'].xcom_push(key=USERS, value=task)
    return task

def get_group(group_id):
    """
    그룹 정보
    """
    db = MySqlHook(mysql_conn_id='mariadb', schema="dbo")
    sql = []
    sql.append(f"""
    select
        group_id, parent_id, name, culture, group_code, sequence, is_childs, depth, is_display, email, interface_id, remark, created_date, modified_date 
    from
        dbo.groups
    where 
        group_id = %s
    """)
    task = {}
    rows = db.get_records(''.join(sql), parameters=[group_id])
    for row in rows:
        model = {
            'group_id':row[0],
            'parent_id':row[1],
            'name':row[2],
            'culture':row[3],
            'group_code':row[4],
            SEQ:row[5],
            'is_childs':row[6],
            'depth':row[7],
            'is_display': row[8],
            'email':row[9],
            'remark':row[10],
            'created_date':str(row[11]),
            'modified_date':str(row[12])
        }
        task = model

    # context['ti'].xcom_push(key=GROUPS, value=task)
    return task

def get_group_users(gid, uid):
    """
    그룹에 소속된 사용자 정보
    """
    GROUP_USERS = 'group_users'
    db = MySqlHook(mysql_conn_id='mariadb', schema="dbo")
    sql = []
    sql.append(f"""
    select 
        b.*, a.relation_type, a.is_master
    from 
        dbo.group_users a
        inner join dbo.groups b
        on a.group_id = b.group_id
    where 
        a.user_id = %s and a.parent_id = %s
    """)
    tasks = {}
    tasks[GROUP_USERS] = []
    rows = db.get_records(''.join(sql), parameters=[uid, gid])
    for row in rows:
        model = {
            'group_id':row[0],
            'parent_id':row[1],
            'name':row[2],
            'culture':row[3],
            'group_code':row[4],
            'sequence':row[5],
            'is_childs':row[6],
            'depth':row[7],
            'is_display': row[8],
            'email':row[9],
            'remark':row[10],
            'created_date':str(row[11]),
            'modified_date':str(row[12]),
            'relation_type':row[13],
            'is_master':row[14]
        }
        tasks[GROUP_USERS].append(model)

    # context['ti'].xcom_push(key=GROUPS, value=tasks[GROUP_USERS])
    return list(tasks.values())    

# 결재선
def get_signers(instance_id):
    db = MySqlHook(mysql_conn_id='mariadb', schema="dapp")
    sql = []
    sql.append(f"""
    select
        a.instance_id, a.sign_area_id, sequence, sub_instance_id, sign_section, sign_position, sign_action, is_executed, group_culture, group_id, group_name, created_date, received_date, approved_date, b.sign_area
    from
        dapp.signers a
    inner join  
        dapp.sign_areas b
    on a.instance_id=b.instance_id and a.sign_area_id=b.sign_area_id
    where 
        a.instance_id=%s
    """)
    tasks = {}
    rows = db.get_records(''.join(sql), parameters=[instance_id])
    tasks[SIGNERS] = []
    for row in rows:
        model = {
            'instance_id':row[0],
            'sign_area_id':row[1],
            'sequence':row[2],
            'sub_instance_id':row[3],
            'sign_section':row[4],
            'sign_position':row[5],
            'sign_action':row[6],
            'is_executed':row[7],
            'group_culture': row[8],
            'group_id':row[9],
            'group_name':row[10],
            'created_date':str(row[11]),
            'received_date':str(row[12]),
            'approved_date':str(row[13]),
            'sign_area':row[14]
        }
        tasks[SIGNERS].append(model)

    return list(tasks.values())


# 결재 영역
def get_sign_areas(instance_id):
    db = MySqlHook(mysql_conn_id='mariadb', schema="dapp")
    sql = []
    sql.append(f"""
    select
        instance_id, sign_area_id, sign_area
    from
        dapp.sign_areas
    where 
        instance_id=%s
    """)
    tasks = {}
    rows = db.get_records(''.join(sql), parameters=[instance_id])
    tasks[SIGN_AREA] = []
    for row in rows:
        model = {
            'instance_id':row[0],
            'sign_area_id':row[1],
            'sign_area':row[2]
        }
        tasks[SIGN_AREA].append(model)

    return list(tasks.values())    

# 결재선 사용자
def get_sign_users(instance_id):
    db = MySqlHook(mysql_conn_id='mariadb', schema="dapp")
    sql = []
    sql.append(f"""
    select
        a.instance_id, a.sign_area_id, sequence, user_culture, user_id, user_name, responsibility, position, class_position, host_address, reserved_date, delay_time, is_deputy, is_comment, b.sign_area
    from
        dapp.sign_users a
    inner join  
        dapp.sign_areas b
    on a.instance_id=b.instance_id and a.sign_area_id=b.sign_area_id
    where 
        a.instance_id=%s
    """)
    tasks = {}
    rows = db.get_records(''.join(sql), parameters=[instance_id])
    tasks[SIGN_USERS] = []
    for row in rows:
        uid = row[4]
        model = {
            'instance_id':row[0],
            'sign_area_id':row[1],
            'sequence':row[2],
            'user_culture':row[3],
            'user_id':uid,
            'user_name':row[5],
            'responsibility':row[6],
            'position':row[7],
            'class_position': row[8],
            'host_address':row[9],
            'reserved_date':str(row[10]),
            'delay_time':row[11],
            'is_deputy':row[12],
            'is_comment':row[13],
            'sign_area':row[14]
        }
        tasks[SIGN_USERS].append(model)

    return list(tasks.values())

## 등록 스크립트 
def insert_signers(lst):
    """
    sql:결재선 등록
    """    
    sql = []
    sql.append('insert into dapp.signers(instance_id, sign_area_id, sequence, sub_instance_id, sign_section, sign_position, sign_action, is_executed, group_culture, group_id, group_name, created_date, received_date, approved_date) values')
    index = 0
    sub_instance_id = 0
    for m in lst:
        index += 1
        colon = ''
        if len(lst) > index:        
            colon = ','
        sql.append("""({},{},'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'){}""".format(
            m['instance_id'],
            m['sign_area_id'],
            m[SEQ],
            sub_instance_id,
            m['sign_section'],
            m['sign_position'],
            m[SIGN_ACTION],
            m[IS_EXECUTED],
            m['culture'],
            m['group_id'],
            add_quote(m[GROUP_NAME]),
            datetime.utcnow(),
            datetime.utcnow(),
            datetime.utcnow(),
            colon))
    sql.append(';')
    return ''.join(sql)

def insert_post_office(lst):
    """
    sql:결재 진행함
    """    
    sql = []
    sql.append('insert into dapp.post_office(box_id,participant_id,instance_id,row_version,name,participant_name,is_viewed,location,created_date) values')
    index = 0
    is_viewed = 0
    for m in lst:
        index += 1
        colon = ''
        if len(lst) > index:        
            colon = ','
        sql.append("""({},'{}',{},'{}','{}','{}','{}','{}','{}'){}""".format(
            m['box_id'],
            m['participant_id'],
            m['instance_id'],
            m['row_version'],
            m['name'],
            add_quote(m['participant_name']),
            is_viewed,
            m['location'],
            datetime.utcnow(),
            colon))
        sql.append(';')
    return ''.join(sql)

def insert_post_boxes(lst):
    """
    sql:결재 완료함
    """    
    sql = []
    sql.append('insert into dapp.post_boxes(box_id,participant_id,instance_id,row_version,name,participant_name,is_viewed,location,created_date) values')
    index = 0
    is_viewed = 0
    for m in lst:
        index += 1
        colon = ''
        if len(lst) > index:        
            colon = ','
        sql.append("""({},'{}',{},'{}','{}','{}','{}','{}','{}'){}""".format(
            m['box_id'],
            m['participant_id'],
            m['instance_id'],
            m['row_version'],
            m['name'],
            add_quote(m['participant_name']),
            is_viewed,
            m['location'],
            datetime.utcnow(),
            colon))
    sql.append(';')
    return ''.join(sql)

def insert_sign_users(lst):
    """
    sql:결재선 사용자 등록
    """       
    sql = []
    sql.append("""insert into dapp.sign_users(instance_id, sign_area_id, sequence, user_culture, user_id, user_name, responsibility, position, class_position, host_address, reserved_date, delay_time, is_deputy, is_comment) values""")
    index = 0
    for m in lst:
        index += 1
        colon = ''
        if len(lst) > index:        
            colon = ','
        uid = m['user_id']
        gid = m['group_id']
        m['responsibility'] = ''
        m['position'] = ''
        m['class_position'] = ''
        if m['host_address'] == None:
            m['host_address'] = ''
        m['reserved_date'] = 'null'
        m['is_deputy'] = 0
        group_users = get_group_users(gid, uid)
        if group_users:
            for n in group_users[0]:
                if n['relation_type'] == 1:
                    m['responsibility'] = n['group_id']
                elif n['relation_type'] == 2:
                    m['position'] = n['group_id']
                elif n['relation_type'] == 3:
                    m['class_position'] = n['group_id']
        sql.append("""({},{},'{}','{}','{}','{}','{}','{}','{}','{}',{},'{}',{},{}){}""".format(
            m['instance_id'],
            m['sign_area_id'],
            m['sequence'],
            m['culture'],
            m['user_id'],
            add_quote(m['name']),
            m['responsibility'],
            m['position'],
            m['class_position'],
            m['host_address'],
            m['reserved_date'],
            m['delay_time'],
            m['is_deputy'],
            m['is_comment'],
            colon))
    sql.append(';')
    return ''.join(sql)

def delete_activity(instance_id, sign_area, sequence):
    """
    sql:현 결재자 삭제
    """    
    sql = []
    sql.append('''delete from dapp.sign_activity where instance_id={} and sign_area='{}' and sequence={}'''.format(instance_id, sign_area, sequence))
    sql.append(';')
    return ''.join(sql)

def delete_activity_all(instance_id):
    """
    sql:대기중인 결재자 모두 삭제
    """
    sql = []
    sql.append('delete from dapp.sign_activity where instance_id={};'.format(instance_id))
    return ''.join(sql)    

def update_activity(lst):
    """
    sql:현 결재자 지정
    """
    sql2 = []
    instance_id = index = is_viewed = 0    
    sign_area = ''
    for m in lst:
        if instance_id == 0:
            instance_id = m[INSTANCE_ID]
        if sign_area == '':
            sign_area = m[SIGN_AREA]
        index += 1
        colon = ''
        if len(lst) > index:        
            colon = ','
        sql2.append("""{}{}""".format(
            m[SEQ],
            colon))
        sql2.append(');')
    sql = []
    sql.append('''update dapp.sign_activity set sign_action='{}' where instance_id={} and sign_area='{}' and sequence in ('''.format(STATUS_01, instance_id, sign_area))
    sql.append(''.join(sql2))
    return ''.join(sql)

def complete_workflow(workflow_process_id):
    """
    sql:결재 완료 workflow 프로세스 진행
    """
    sql = []
    sql.append('insert into djob.workflow_process_cmpl ')
    sql.append("select workflow_process_id, ngen, site_id, application_id, instance_id, schema_id, name, workflow_instance_id, state, retry_count, ready, '{}' execute_date,".format(datetime.utcnow())) 
    sql.append("created_date, '{}' bookmark, version, request, reserved, message from djob.workflow_process where workflow_process_id={};".format(BOOKMARK, workflow_process_id))
    sql.append("delete from djob.workflow_process where workflow_process_id={};".format(workflow_process_id))
    return ''.join(sql)

def add_quote(value):
    """
    sql:값 직접 등록 시 single quote 처리
    """
    return value.replace("'","\\'")

def date_diff_in_seconds(dt2, dt1):
  timedelta = dt2 - dt1
  return timedelta.days * 24 * 3600 + timedelta.seconds