from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta,time,date
from airflow.models import Variable

import connections_airflow as dfos_con
import airflow_library as bot_func
import connections_airflow_write as dfos_con_write

from productised_code_v4.company_name.productised_code_v4.tasks.submission_mail_for_single_form import main_code_submission_mail_for_single_form
from productised_code_v4.company_name.productised_code_v4.tasks.submission_mail_for_multiple_forms import main_code_submission_mail_for_multiple_forms
from productised_code_v4.company_name.productised_code_v4.tasks.workflow_approval_status import main_code_for_workflow_approval_status
from productised_code_v4.company_name.productised_code_v4.tasks.basic_schedule_form import main_code_basic_schedule_form
from productised_code_v4.company_name.productised_code_v4.tasks.form_submission_on_planning import main_code_form_submission_on_planning
from productised_code_v4.company_name.productised_code_v4.tasks.nc_assign_based_on_master_form import main_code_nc_assign_based_on_master_form
# company_name/project_name/tasks/form_submission_on_planning.py
import json

def main_parser(**kwargs):
    
    # Access the config dictionary from the DAG run context
    params = kwargs.get('dag_run').conf
    
    # Extract values from the config
    audit_id =  params['audit_id']                              
    form_id = params['form_id'] 
    url = params['url'] 

    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Print/log key variable values for debugging and auditing purposes
    # Helps trace issues if something fails during execution
    
    print(f"::group:: PARAMS =====")
    print("Audit ID:- ", audit_id)
    print("Form ID:- ", form_id)
    print("URL:- ", url)
    print("Params:- ", params)
    print("::endgroup::")

    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    kwargs['ti'].xcom_push(key="parsed_conf", value=params)


    return "DesignX"

# Optional fallback no-op task
no_op = BashOperator(
    task_id="no_op",
    bash_command="echo 'No valid tasks in config. Skipping...'"
)

def branch_decider(**kwargs):
    ti = kwargs['ti']
    dag = kwargs['dag']
    conf = ti.xcom_pull(key="parsed_conf", task_ids='pasres_the_json') or {}

    all_task_ids = dag.task_ids
    print("All DAG task_ids:", all_task_ids)
    print("Conf keys:", conf.keys())

    matched_tasks = [key for key in conf if key in all_task_ids]

    return matched_tasks if matched_tasks else "no_op"


def task_wrapper(task_key, task_callable):
    def _wrapped(**kwargs):
        ti = kwargs['ti']
        conf = ti.xcom_pull(key="parsed_conf", task_ids='pasres_the_json') or {}

        global_config = {
            "audit_id": conf.get("audit_id"),
            "form_id": conf.get("form_id"),
            "url": conf.get("url"),
            "conf": conf.get("conf")
        }

        task_config = conf.get(task_key, {})

        if not task_config:
            print(f"[{task_key}] Config not found in conf. Skipping task logic.")
            return f"Skipped {task_key}"

        combined_config = {**global_config, **task_config}
        print(f"[{task_key}] Running with combined config:")
        print(json.dumps(combined_config, indent=4))
        return task_callable(**combined_config)
    return _wrapped
# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# The following code is a mandatory structure for defining an Airflow DAG. It contains essential configurations for Airflow to recognize and manage this workflow.
# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
default_args={
    'owner': 'Aviral_Tanwar',
    'retries': 0,
}

with DAG(
    dag_id='CompanyName_GlobalStandardBot_main_json_parser',                      # This should be unique across all dags on that VM. Make sure to follow the dag nomenclature mentioned in the ppt.
    default_args=default_args,
    # schedule_interval='* * * * *',                            # If the code needs to run on a schedule/cron, it is defined here. In this case, it means run every minute
    schedule = None,                                            # If there is no scheduling
    catchup=False,                                              # It ensures that the DAG does not backfill missed runs between the start_date and the current date. Only the latest scheduled run will be executed.
    max_active_runs=1,                                          # Ensures only one active run at a time
    # concurrency=1,                                              # Only one task instance can run concurrently
    tags = ["Productized_Code", "Company_Name","JSON_Parser" "Aviral_Tanwar"],
    default_view='graph',) as dag:                        
        

    json_fetch = PythonOperator(
        task_id="pasres_the_json",
        python_callable=main_parser
    )

    no_op = BashOperator(
    task_id="no_op",
    bash_command="echo 'No valid tasks matched. Skipping.'"
    )
    
    branch_decision = BranchPythonOperator(
        task_id="branch_decision",
        python_callable=branch_decider,
        provide_context=True
    )

    submission_mail_for_single_form = PythonOperator(
        task_id='submission_mail_for_single_form',
        python_callable=task_wrapper('submission_mail_for_single_form', main_code_submission_mail_for_single_form)
    )

    submission_mail_for_multiple_forms = PythonOperator(
        task_id='submission_mail_for_multiple_forms',
        python_callable=task_wrapper('submission_mail_for_multiple_forms', main_code_submission_mail_for_multiple_forms)
    )

    workflow_approval_status = PythonOperator(
        task_id='workflow_approval_status',
        python_callable=task_wrapper('workflow_approval_status', main_code_for_workflow_approval_status)
    )

    basic_schedule_form = PythonOperator(
        task_id='basic_schedule_form',
        python_callable=task_wrapper('basic_schedule_form', main_code_basic_schedule_form)
    )

    form_submission_on_planning = PythonOperator(
        task_id='form_submission_on_planning',
        python_callable=task_wrapper('form_submission_on_planning', main_code_form_submission_on_planning)
    )

    nc_assign_based_on_master_form = PythonOperator(
        task_id='nc_assign_based_on_master_form',
        python_callable=task_wrapper('nc_assign_based_on_master_form', main_code_nc_assign_based_on_master_form)
    )

    json_fetch >> branch_decision >> [
    submission_mail_for_single_form,
    submission_mail_for_multiple_forms,
    workflow_approval_status,
    basic_schedule_form,
    form_submission_on_planning,
    nc_assign_based_on_master_form,
    no_op
]
