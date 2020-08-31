from airflow.plugins_manager import AirflowPlugin
from wizen_plugin.sensors.workflow_sensors import WorkflowSensor
from wizen_plugin.operators.signers_operator import SignersOperator

class WizenPlugin(AirflowPlugin):
    name = "wizen_plugin"
    operators = [SignersOperator]
    sensors = [WorkflowSensor]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []