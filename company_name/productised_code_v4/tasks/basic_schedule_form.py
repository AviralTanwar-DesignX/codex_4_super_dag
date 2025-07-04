#####
# --------------------------------------------------------------------------------
# THIS TASK'S WORKING:
#       This task is used to schedule a form based on responses submitted in a main form. It supports both dynamic and static configurations for form ID, module ID, users, dates, and more.
# --------------------------------------------------------------------------------

# ----------------------
# PRE-REQUISITES by DFOS-DEVELOPER:
# ----------------------
# 1) NONE

# ----------------------
# HOW TO ATTACH THE BOT:
# ----------------------
#       1) BOT NAME: Any display name for DFOS
#       2) BOT URL Format: 
#           https://airflowprod.dfos.co:8080/api/v1/dags/{dag_id}/dagRuns
#               Example:  https://airflowprod.dfos.co:8080/api/v1/dags/CompanyName_ProjectName_GlobalStandardBot_workflow_approval_status/dagRuns
#       3) BOT CONF (JSON Format):
#           {"basic_schedule_form": {"module_field_id": "","module_id": "","form_field_id": "","form_id_schedule": "","schedule_field_users": "","schedule_static_users": "","bot_user_id": "","start_date_field_id": "","start_date": "","end_date_field_id": "","end_date": "","approver_field_id": "","card_details": ""}}
#               * `module_field_id`: 	(Optional) Question ID from which the module ID should be dynamically fetched. Only one value is allowed.
#               * `module_id`:  	(Optional) Static module ID of the form to be scheduled. Required if module_field_id is not used. Only one value is allowed.
#               * `form_field_id`: 	(Optional) Question ID from which the form ID should be dynamically fetched. Only one value is allowed.
#               * `form_id_schedule`: 	(Optional) Static form ID of the form to be scheduled. Required if form_field_id is not used. Only one value is allowed.
#               * `schedule_field_users`: (Optional) Question ID from which the user IDs should be dynamically fetched. Only one value is allowed.
#               * `schedule_static_users`:	(Optional) Comma-separated static user IDs to assign the scheduled form to.
#               * `bot_user_id`: 	(Required) ID of the bot user responsible for the scheduling action.
#               * `start_date_field_id`: 	(Optional) Static start date for the scheduled form. Required if start_date_field_id is not used. 
#               * `start_date`: If it's static, the it must be passed here i.e. the start date of the form which needs to be scheduled. 
#               * `end_date_field_id`: (Optional) Question ID from which the end date should be dynamically fetched.
#               * `end_date`:  (Optional) Static end date for the scheduled form. Required if end_date_field_id is not used. It is in number and logic is ( no of days+ Start Date)
#               * `approver_field_id`: If there is a secondary form, then the question id where its being approved or rejected needs to be here
#               * `card_details`: All the question ids which needs to be shown on the card in the schedule option

# ----------------------
# REQUIREMENTS:
# ----------------------
# 1) > The workflow for this BOT involves a **main form** that contains questions to configure the scheduling of **secondary forms**.  
# 2)Based on the submitted values, the BOT either **approves or rejects** and schedules follow-up forms accordingly.
# 3)If `module_field_id` is provided, `module_id` **must** be an empty string (`""`), and **vice versa**.
# 4)If `form_field_id` is provided, `form_id_schedule` **must** be an empty string, and **vice versa**.
# 5)If `start_date_field_id` is provided, `start_date` **must** be an empty string, and **vice versa**.
# 6)If `end_date_field_id` is provided, `end_date` **must** be an empty string, and **vice versa**.
# ----------------------

# ----------------------
# WORKING:
# ----------------------
#       1) This DAG recieves the form id for which it is triggered
#       2) The XBot finds the Linked Main Form of the form
#       3) It changes the Status of the Main form based on the WorkFLow Status Provided
# ----------------------

# ----------------------
# STANDARD CONVENTIONS:
# ----------------------
#       1) In each SQL query, select the primary ID column as NAME_DAG_id
#       2) Add tags at the end of the DAG script:
#           tags = ["CompanyName", "ProjectName", "Type of DAG", "OWNER"]
#       3) Set the Airflow owner to your username and include it in the tags
# ----------------------

# ----------------------
# DEVELOPER REFERENCE:
# ----------------------
#       To see which mail to send use  ->  https://designxpvt-my.sharepoint.com/:w:/g/personal/aviral_tanwar_designx_in/ESHtbhTC9sNPq_N03erp_coBXU8AL9edOJAoeI5kZq3bGQ?e=69GQm7
#       (Depreciared) To see which connection to use ->  https://designxpvt-my.sharepoint.com/:x:/g/personal/aviral_tanwar_designx_in/EQu03bVKGXtCjSdIjddPq_0BMc4b2cjdgSj3H9XR5zk6hw?e=CHOo61
# --------------------------------------------------------------------------------

# -------------------------------------
# Standard Airflow and Python Libraries
# -------------------------------------
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# -------------------------------------
# Custom Internal Libraries
# -------------------------------------
import airflow_library as bot_func                    # For common bot-related utilities
import connections_airflow as dfos_con                # For DB read connections
import connections_airflow_write as dfos_con_write    # For DB write connections

# -------------------------------------
# Commonly used Python Libraries
# -------------------------------------
import json
import requests
from datetime import datetime, timedelta

# --------------------------------------------------------------------------------
# AUTHORSHIP
# Created by: Aviral Tanwar
# Copyright: DesignX DFOS
# --------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------FUNCTIONS WHICH WILL BE CALLED BY THE MAIN CODE IS WRITTEN BELOW--------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def fetch_approver(connection, audit_id, approver_field_id):
    try:
        cur = connection.cursor()

        query = '''
            SELECT ans.fvf_main_field_type
            FROM form_via_form_main_audit_answers AS ans
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''

        cur.execute(query, (audit_id,approver_field_id))
        result = cur.fetchall()

        if result[0][0].lower() == "radio":
            print("Type of Field is : - ", result[0][0])
            
            query = '''
            SELECT ans.answer_option_value
            FROM form_via_form_main_audit_answers AS ans
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''
            cur.execute(query, (audit_id,approver_field_id))
            result = cur.fetchall()

            return result[0][0]
        else:
            
            print("Type of Field is : - ", result[0][0])
            
            query = '''
            SELECT op.fvf_main_field_option_value
            FROM form_via_form_main_audit_answers AS ans
            JOIN form_via_form_main_field_options AS op ON op.fvf_main_field_option_id = ans.fvf_main_field_option_id
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''
            cur.execute(query, (audit_id,approver_field_id))
            result = cur.fetchall()

            return result[0][0]

    except Exception as e:
        print("Error occured in the func fetch_approver:- ", e)
        raise

def fetch_value_id(connection, audit_id, module_field_id):
    try:
        cur = connection.cursor()

        query = '''
            SELECT ans.fvf_main_field_type
            FROM form_via_form_main_audit_answers AS ans
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''

        cur.execute(query, (audit_id,module_field_id))
        result = cur.fetchall()

        if result[0][0].lower() == "radio":
            print("Type of Field is : - ", result[0][0])
            
            query = '''
            SELECT ans.answer_option_value
            FROM form_via_form_main_audit_answers AS ans
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''
            cur.execute(query, (audit_id,module_field_id))
            result = cur.fetchall()

            return result[0][0]
        elif result[0][0].lower() == "dropdown":
            print("Type of Field is : - ", result[0][0])
            
            query = '''
            SELECT op.fvf_main_field_option_value
            FROM form_via_form_main_audit_answers AS ans
            JOIN form_via_form_main_field_options AS op ON op.fvf_main_field_option_id = ans.fvf_main_field_option_id
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''
            cur.execute(query, (audit_id,module_field_id))
            result = cur.fetchall()
        elif result[0][0].lower() == "api":
            print("Type of Field is : - ", result[0][0])
            
            query = '''
            SELECT op.fvf_main_field_option_id
            FROM form_via_form_main_audit_answers AS ans
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''
            cur.execute(query, (audit_id,module_field_id))
            result = cur.fetchall()
        elif result[0][0].lower() == "user":
            print("Type of Field is : - ", result[0][0])
            
            query = '''
            SELECT ans.fvf_main_field_option_id
            FROM form_via_form_main_audit_answers AS ans
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''
            cur.execute(query, (audit_id,module_field_id))
            result = cur.fetchall()
        else:
            print("The module is not found in either Dropdown, API or Radio and hence will fail below!!!")

        return result[0][0]
    except Exception as e:
        print("Error occured in the function fetch_value_id :- ", e)
        raise


def fetch_date(connection,audit_id, date_field_id):
    try:
        
        cur = connection.cursor()

        query = '''
            SELECT ans.answer
            FROM form_via_form_main_audit_answers AS ans
            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id= %s
        '''

        cur.execute(query, (audit_id,date_field_id))
        result = cur.fetchall()

        return result[0][0]

    except Exception as e:
        print("Error occured in the function fetch_date:- ", e)
        raise

def fetch_from_audits(connection, audit_id):
    try:
        cur = connection.cursor()
        sql_query = '''          
                            SELECT ans.module_id as your_name_dag_id, ans.linked_mainaudit_id
                            FROM form_via_form_main_audits AS ans
                            WHERE ans.fvf_main_audit_id = %s;
                            '''
        cur.execute(sql_query, (audit_id))
        result = cur.fetchall()

        return result[0][1]

    except Exception as e:
        print(f"Error occurred in the func => fetch_from_audits: {e}")
        raise  

def fetch_compnay_id(connection, module_id):
    try:
        cur = connection.cursor()
        sql_query = '''          
                            SELECT mo.company_id
                            FROM modules AS mo
                            WHERE mo.module_id = %s
                            '''
            # print(sql_query)
        cur.execute(sql_query, (module_id))
        result = cur.fetchall()

        return result[0][0]

    except Exception as e:
        print(f"Error occurred in the func => fetch_compnay_id: {e}")
        raise  

def get_answers_by_field_names(connection, audit_id, field_ids):
    """
    Returns a dictionary of {field_name: answer} for given audit_id and list of field_ids.
    
    Parameters:
    - cur: PyMySQL cursor object.
    - audit_id: integer.
    - field_ids: list of integers.
    
    Returns:
    - dict: {field_name: answer}
    """
    try:
        cur = connection.cursor()
        if not field_ids:
            return {}
        
        print("Field ids:- ", field_ids)

        placeholders = ', '.join(['%s'] * len(field_ids))
        query = f"""
        SELECT ans.answer, ans.fvf_main_field_name
        FROM form_via_form_main_audit_answers AS ans
        WHERE ans.fvf_main_audit_id = {audit_id} AND ans.fvf_main_field_id IN ({field_ids});
        """

        cur.execute(query)
        results = cur.fetchall()
        print("results:- ", results)

        # PyMySQL with default cursor returns tuples in the same order as selected
        return {field_name: answer for answer, field_name in results}
    except Exception as e:
        print("Error occured in the function get_answers_by_field_names:- ", e)
        raise

def schedule_api(base_url, bot_user_id, module_id, form_id, schedule_user, company_id, start_date, end_date, linked_main_audit_id,schedule_json):
    
    try:
        entry = {
            'schedule_by': str(bot_user_id),
            'Module_id': str(module_id),
            'FORM_ID': str(form_id),
            'scheduled_User': str(schedule_user),
            'Audit_Type': "1",
            'Company_ID': str(company_id),
            'audit date': str(start_date),
            'endDate': str(end_date),
            'Frequency': "Daily",
            'AllowOnce': "1",
            'reference_audit_id': linked_main_audit_id,
            "schedule_json_data":str(schedule_json),
            'schedule_description':"",
 
        }

        print("Posting data to URL")

        url = f"{base_url}api/v3/schedule"
        headers = {'Content-Type': 'application/json'}

        print("API end_point:- ", url)
        print("FORM DATA:- ", json.dumps(entry, indent=4))

        response = requests.post(url, data=json.dumps(entry), headers=headers)

        print("Response Text:", response.text)
        
        if response.status_code == 200:
            print("Data posted successfully")
            print(f"Response Text:- ", response.text)
        else:
            print(f"Failed to post data. Status Code: {response.status_code}")
            print(f"Response Text:- ", response.text)
        return response
    
    except Exception as e:
        print(f"Error occurred while calling the function schedule_api: {e}")
        raise

# **kwargs allows you to pass a variable number of keyword arguments to a function. 
# These arguments are collected into a dictionary where the keys are the argument names and the values are the corresponding values passed.
# This is useful when you want to handle named arguments dynamically, without knowing them beforehand.
def main_code_basic_schedule_form(**kwargs):     

    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Extracting the values sent from the config.... DO NOT CHANGE THIS AT ALL
    # These are the core input parameters used in scheduling the follow-up form based on the values submitted in the current form

    audit_id = kwargs['audit_id']                          # ID of the current audit... Sent by the Product Team and is constant
    form_id = kwargs['form_id']                            # ID of the form that triggered this logic... Sent by the Product Team and is constant
    url = kwargs["url"]                                    # API base URL... Sent by the Product Team and is constant

    module_field_id = kwargs['module_field_id']            # Field ID containing dynamic module selection (optional)
    module_id = kwargs['module_id']                        # Static module ID to use for scheduling
    form_field_id = kwargs['form_field_id']                # Field ID containing dynamic form ID to be scheduled
    form_id_schedule = kwargs['form_id_schedule']          # Static form ID to be scheduled
    schedule_field_users = kwargs['schedule_field_users']  # Field ID with user selection
    schedule_static_users = kwargs['schedule_static_users']# Static list of user IDs (comma-separated)
    bot_user_id = kwargs['bot_user_id']                    # Bot user ID for authentication
    start_date_field_id = kwargs['start_date_field_id']    # Field ID with dynamic start date
    start_date = kwargs['start_date']                      # Static start date (format: YYYY-MM-DD)
    end_date_field_id = kwargs['end_date_field_id']        # Field ID with dynamic end date
    end_date = kwargs['end_date']                          # Static end date (either full date or number of days from start)
    approver_field_id = kwargs['approver_field_id']        # Field ID used to determine form approval/rejection
    card_details = kwargs['card_details']                  # Comma-separated field IDs to be shown on the scheduled form card


    # Handle optional `conf` parameter, which may contain nested configuration including next form ID... Sent by the Product Team and is constant
    if 'conf' in kwargs and kwargs['conf']:
        try:
            conf = json.loads(kwargs['conf']) 
            if isinstance(conf, list) and conf and 'next_form_id' in conf[0]:
                next_form_id = conf[0]['next_form_id'] 
        except json.JSONDecodeError:
            # JSON was not formatted correctly - ignoring safely
            pass


    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Print/log key parameter values for auditing and debugging

    print(f"::group:: PARAMS =====")

    print("Audit Id:- ", audit_id)
    print("Form Id:-  ", form_id)
    print("Field Id where I will find the module Id:- ", module_field_id)
    print("Module Id:- ", module_id)
    print("Field Id where i will find the form id:- ", form_field_id)
    print("Form ID which needs to be shceduled:- ", form_id_schedule)
    print("Schedule Field Users:- ", schedule_field_users)
    print("Schedule Static Users:- ", schedule_static_users)
    print("Bot User ID:- ", bot_user_id)
    print("Start Dtae Field Id:- ", start_date_field_id)
    print("Start Date in Date Format:- ", start_date)
    print("End Date Field ID:- ", end_date_field_id)
    print("End Date (= no of days+ Start Date):- ", end_date)
    print("URL:- ", url)
    print("Approver Field Id:- ", approver_field_id)
    print("card_details:- ", card_details)

    print("::endgroup::")


# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------MAIN CODE'S LOGIC STARTS FROM BELOW------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    print("-------------------------------------------------------------------------------------------------------------------")
    print("-------------------------------------------------------------------------------------------------------------------")


    # -----------------------
    # Step 1: Create DB connection using URL provided
    # -----------------------
    connection = dfos_con.connection_for_all_replica(url)  

    print("-------------------------------------------------------------------------------------------------------------------")

    # -----------------------
    # Step 2: If form is in review, check approval status and exit early if rejected
    # -----------------------
    if approver_field_id == "":
        pass
    else:
        approver_check = fetch_approver(connection,audit_id, approver_field_id)

        if approver_check == 2:
            print("The form has been rejected and hence there will be no SCHEDULING")
            return "DesignX"
        else:    
            audit_id = fetch_from_audits(connection, audit_id)
            print("Now the audit id is:- ", audit_id)
        
    # -----------------------
    # Step 3: Dynamically fetch module ID if field ID was provided
    # -----------------------
    if module_field_id != "":
        module_id = fetch_value_id(connection, audit_id, module_field_id)
    print("Module Id :- ", module_id)

    # -----------------------
    # Step 4: Dynamically fetch form ID to be scheduled if not passed statically
    # -----------------------
    if form_field_id != "":
        form_id_schedule = fetch_value_id(connection, audit_id, form_field_id)
    print("Form Id which needs to be scheduled:- ", form_id_schedule)

    # -----------------------
    # Step 5: Resolve users to assign the form to — combining static and dynamic
    # -----------------------
    if schedule_field_users != "":
        users = fetch_value_id(connection, audit_id, schedule_field_users)

    if schedule_static_users != "":
        schedule_static_users = schedule_static_users +","+users
    else:
        schedule_static_users = users

    print("Users for which the forms needs to be scheduled:- ", schedule_static_users)

    # -----------------------
    # Step 6: Resolve start date from field (if not statically passed)
    # -----------------------
    if start_date_field_id != "":
        start_date = fetch_date(connection,audit_id, start_date_field_id)
    print("Start Date :- ", start_date)

    # -----------------------
    # Step 7: Calculate or fetch end date depending on format
    # -----------------------
    if end_date != "":
        try:
            end_date_value = int(end_date)
            end_date_calc = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=end_date_value)).date()
            print("End Date (= no of days + Start Date):- ", end_date_value)
            end_date = str(end_date_calc)
        except Exception as e:
            print("Error calculating end date:", e)
    elif end_date_field_id != "":
        end_date = fetch_date(connection,audit_id, end_date_field_id)
    
    print("End Date :- ", end_date)

    # -----------------------
    # Step 8: Get linked main audit ID if applicable
    # -----------------------
    linked_main_audit_id = fetch_from_audits(connection, audit_id)
    
    # -----------------------
    # If there's no linked main audit (i.e., value is 0), default to the current audit ID
    # -----------------------
    if linked_main_audit_id == 0:
        linked_main_audit_id = audit_id
    print("Linked Main Audit ID:", linked_main_audit_id)

    # -----------------------
    # Step 9: Fetch company ID using module ID (used in submission metadata)
    # -----------------------
    company_id = fetch_compnay_id(connection, module_id)
    print("Company Id:- ", company_id)

    # -----------------------
    # Step 10: Prepare card metadata (question answers) to display on scheduled card
    # -----------------------
    schedule_json = get_answers_by_field_names(connection, audit_id, card_details)
    schedule_json = json.dumps(schedule_json)
    print("Schedule Json which will be shwon on the card:- ", schedule_json)
    
    # -----------------------
    # Step 11: Call the API to schedule the form with all final parameters
    # -----------------------
    response = schedule_api(url, bot_user_id, module_id, form_id_schedule, schedule_static_users, company_id, start_date, end_date, linked_main_audit_id, schedule_json)
    print(response)

    # -----------------------
    # Step 12: Cleanup - Close DB connection
    # -----------------------
    connection.close()

    # -----------------------
    # Step 13: Return final response to DAG
    # -----------------------
    return "DesignX"