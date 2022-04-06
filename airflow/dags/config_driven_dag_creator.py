from datetime import datetime, timedelta
from airflow.models import DAG
import sys, os, yaml
from pathlib import Path

pwd = str(Path(__file__).parents[1])
sys.path.insert(0, pwd) #TODO: this path can be set in a global config file
from util.common import create_dag_tasks, DEFAULT_ARGS, slack_alert_sla_missed, update_sla_durations


dag_config_path=os.path.join(pwd,'dag_configs')

for file in os.listdir(dag_config_path):
    #if not a yaml config file, skip
    if file.endswith(".yaml") == False: continue

    dag_config_file=os.path.join(dag_config_path,file)
    try:
        with open(dag_config_file, 'r') as stream:
            dag_config = yaml.safe_load(stream)
    except yaml.YAMLError as err:
        print ("\nError while loading YAML file: {}, next...".format(dag_config_file))
        continue
    
    # Dynamic SLA Finding Functionality: SLA values updated every time a DAG is constructed (type: dictionary)
    sla_durations = update_sla_durations(dag_config["dag_name"])
    # sla_durations = {} # Set to empty dictionary to disable sla for YAML files for now, uncomment line above and delete this line to re-enable slas. 

        
    # DAG Definition
    try:
        DAG_ID = dag_config["dag_name"]
        globals()[DAG_ID] = DAG(DAG_ID,
                default_args=DEFAULT_ARGS,
                start_date=datetime.strptime(dag_config["dag_start_date"],"%Y-%m-%d"),
                schedule_interval=dag_config["dag_schedule"],
                max_active_runs=1,
                catchup=dag_config["catchup"],
                sla_miss_callback = slack_alert_sla_missed 
                )
    except KeyError as err:
        print ("\nError parsing key-value pairs in config {}; details: {}, next...".format(dag_config_file, str(err)))
        continue

    print ("\nLoaded YAML for dag {}, calling task constructor...".format(DAG_ID))
    create_dag_tasks(globals()[DAG_ID], dag_config, sla_durations)