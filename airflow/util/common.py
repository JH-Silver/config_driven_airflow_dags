def create_dag_tasks(dag, dag_config, sla_durations=None):
    print("\nCreating Tasks...")
    dag_tasks={}
    task_dependencies={}
    try:
        for task in dag_config['tasks']:
            dag_task_config=dag_config['tasks'][task]
            trigger_rule= dag_task_config["trigger_rule"] if dag_task_config.get("trigger_rule") else "all_success"
            task_id= task
            sla_timedelta = get_sla_value(sla_durations, task_id)
            
            #load script/job type specific params
            if dag_task_config["job_type"] == "nodejs":
                bash_cmd= #TODO: bash template                
                #create dag tasks
                dag_tasks[task_id]=BashOperator(task_id=task_id,
                                                bash_command=bash_cmd,
                                                trigger_rule=trigger_rule,
                                                sla = sla_timedelta, #TODO
                                                dag=dag)
            #TODO Delete this test case line 288 to 301
            elif dag_task_config["job_type"] == "python":
                #create dag tasks
                dag_tasks[task_id]=BashOperator(task_id=task_id,
                                                  bash_command=bash_cmd,
                                                  trigger_rule=trigger_rule,
                                                  sla = sla_timedelta, #TODO
                                                  dag=dag)
            elif dag_task_config["job_type"] == "pyspark":
                #create dag tasks
                dag_tasks[task_id]=BashOperator(task_id=task_id,
                                                  bash_command=bash_cmd,
                                                  trigger_rule=trigger_rule,
                                                  sla = sla_timedelta, #TODO
                                                  dag=dag)
            


            #add dependency maps
            if dag_task_config.get('dependencies'):
                task_dependencies[task_id]=dag_task_config['dependencies']
            if dag_task_config.get('cross_dag_dependencies'):
                print("\n\tCross-dag dependencies detected, building out task sensors...")
                #loop through dependencies, parse out dag name.task, create sensor task, and replace task name in the task dependencies table
                for upstream_dependency in dag_task_config['cross_dag_dependencies']:
                    upstream_dag, upstream_task= upstream_dependency.split(".")
                    sensor_task_id = upstream_task + "_ext_sensor"
                    upstream_sensor_settings = dag_task_config['cross_dag_dependencies'][upstream_dependency]
                    execution_delta = timedelta(**upstream_sensor_settings['execution_delta']) if upstream_sensor_settings.get('execution_delta') else None
                    # create an ext task sensor if not already exist
                    if dag_tasks.get(sensor_task_id) is None: 
                        dag_tasks[sensor_task_id] = ExternalTaskSensor(
                            task_id=sensor_task_id,
                            external_dag_id=upstream_dag,
                            external_task_id=upstream_task,
                            timeout=upstream_sensor_settings['timeout'], #5 min timeout, execution_delta is defaulting to zero
                            execution_delta=execution_delta,
                            check_existence=True)
                    task_dependencies[task_id] = task_dependencies[task_id] + [sensor_task_id] if task_dependencies.get(task_id) else [sensor_task_id]

        #set up dependencies
        print("\nBuilding task dependencies: {}".format(task_dependencies))
        for task in task_dependencies:
            for upstream_task in task_dependencies[task]:
                dag_tasks[task].set_upstream(dag_tasks[upstream_task])
    except KeyError as err:
        print ("\nError parsing key-value pairs in YAML for {}; details: {}, next...".format(dag_config["dag_name"], str(err)))
