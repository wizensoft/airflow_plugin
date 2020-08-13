from airflow.plugins_manager import AirflowPlugin
from wizen_plugin.sensors.workflow_sensors import WorkflowSensor

class WizenPlugin(AirflowPlugin):
    name = "wizen_plugin"
    operators = []
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