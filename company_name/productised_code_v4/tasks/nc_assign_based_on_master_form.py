# Created By Bavita


from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta

import connections_airflow as dfos_con
import airflow_library as bot_func
import connections_airflow_write as dfos_con_write

import requests




# def table_update(token, url, approval_type, audit_id, user_id_bot):
def table_update(token, base_url, user_id, user_id_bot, audit_id, ans_id):
    url = f"{base_url}api/v3/update_data?table=form_via_form_main_audit_answers&field=task_responsibility_id&value={user_id}&condField=fvf_main_ans_id&condValue={ans_id}&user_id={user_id_bot}"


    payload = {}
    files={}
    headers = {
    'Authorization': f'{token}'
    }

    response = requests.request("GET", url, headers=headers, data=payload, files=files)

    print(response.text)

    try:
        print("Sending request to:", url)
        response = requests.request("GET", url, headers=headers, data=payload, files=files)
        print("Response status code:", response.status_code)
        response.raise_for_status()
        print("Response JSON:", response.json())
        return response.json()
    except requests.exceptions.RequestException as e:
        print("API request failed:", e)
        return None


################################################### [02/07/2025]##
def get_nc_status(connection, audit_id, form_id):
    try:
        cur = connection.cursor()        
        sql_query = """ SELECT ans.fvf_main_ans_id, ans.task_responsibility_id, ans.fvf_main_field_name
                    FROM form_via_form_main_audit_answers AS ans 
                    WHERE ans.fvf_main_audit_id=%s
                    AND ans.fvf_main_form_id=%s
                    AND ans.is_nc_marked=1
                    AND ans.deleted_at="0000-00-00 00:00:00"; """

        cur.execute(sql_query, (audit_id, form_id))
        result = cur.fetchall()
        if not result:
            print("nc_status result is empty")
            return None
        print("nc_status result - ",result)
        return result
    except Exception as e:
        print(f"Error occurred while fetching data from get_nc_status_mf function: {e}")
        raise
    
def get_opt_value(connection, audit_id):
    try:
        cur = connection.cursor()        
        sql_query = """ SELECT opt.fvf_main_field_option_value
FROM form_via_form_main_audit_answers AS ans
LEFT JOIN form_via_form_main_field_options AS opt
ON opt.fvf_main_field_option_id = ans.fvf_main_field_option_id
WHERE ans.fvf_main_audit_id = %s
AND ans.deleted_at = "0000-00-00 00:00:00"; """

        cur.execute(sql_query, (audit_id))
        result = cur.fetchall()
        return result[0][0]
    except Exception as e:
        print(f"Error occurred while fetching data from get_opt_value function: {e}")
        raise
    
def get_master_audit(connection, univ_master_form, opt_value):
    try:
        cur = connection.cursor()        
        sql_query = """ SELECT ans.fvf_main_audit_id, ans.answer, opt.fvf_main_field_option_value
FROM form_via_form_main_audit_answers AS ans
LEFT JOIN form_via_form_main_field_options AS opt
ON opt.fvf_main_field_option_id = ans.fvf_main_field_option_id
LEFT JOIN form_via_form_main_forms AS main
ON main.fvf_main_form_id = ans.fvf_main_form_id
WHERE main.universal_tag_id = %s
AND opt.fvf_main_field_option_value = %s
AND ans.deleted_at = "0000-00-00 00:00:00"
AND opt.deleted_at = "0000-00-00 00:00:00"
AND main.deleted_at = "0000-00-00 00:00:00"
ORDER BY ans.fvf_main_audit_id DESC LIMIT 1;"""

        cur.execute(sql_query, (univ_master_form, opt_value))
        result = cur.fetchall()
        return result[0][0]
    except Exception as e:
        print(f"Error occurred while fetching data from get_master_audit function: {e}")
        raise
    
def get_user_id(connection, master_audit, univ_user_fld_tag):
    try:
        cur = connection.cursor()        
        sql_query = """ SELECT ans.fvf_main_field_option_id
FROM form_via_form_main_audit_answers AS ans
LEFT JOIN form_via_form_main_fields AS fld
ON fld.fvf_main_field_id = ans.fvf_main_field_id
WHERE ans.fvf_main_audit_id = %s
AND fld.universal_tag_id = %s
AND ans.deleted_at = "0000-00-00 00:00:00"
AND ans.deleted_at = "0000-00-00 00:00:00"; """

        cur.execute(sql_query, (master_audit, univ_user_fld_tag))
        result = cur.fetchall()
        if not result:
            print(f"user not found in master form")
            return None
        # Extract the first value from each row and join by comma
        user_ids = ",".join(str(row[0]) for row in result if row[0] is not None)
        return user_ids
    except Exception as e:
        print(f"Error occurred while fetching data from get_user_id function: {e}")
        raise

def get_authorization(connection, user_id_bot):
    try:
        cur = connection.cursor()
        sql_query = '''
                        SELECT u.Authorization FROM users u 
                        WHERE u.user_id = %s
                        AND u.deleted_at='0000-00-00 00:00:00';'''
        print(sql_query)
        cur.execute(sql_query, (user_id_bot,))  # Passing audit_id as a parameter
        result = cur.fetchall()
        return result[0][0]
   
    except Exception as e:
        print(f"Error occurredin the func get_authorization: {e}")
        return None    
               
def main_code_nc_assign_based_on_master_form(**kwargs):       
    # params = kwargs.get('dag_run').
    
    form_id = kwargs['form_id']
    audit_id = kwargs['audit_id']
    base_url = kwargs['url']
    user_id_bot = kwargs['bot_user_id']
    univ_master_form = kwargs['universal_tag_of_master_form']
    univ_user_fld_tag = kwargs['universal_tag_of_field_id_for_user']
    
    print(f"::group:: PARAMS =====")
    print(f"base_url =  {base_url}")
    print(f"user_id_bot = {user_id_bot}")
    print(f"form_id = {form_id}")
    print(f"audit_id = {audit_id}")
    print(f"univ_master_form = {univ_master_form}")
    print(f"univ_user_fld_tag = {univ_user_fld_tag}")
    print("::endgroup::")
    
    
    connection = dfos_con.connection_for_all_master(base_url)
    print(f"connection = {connection}")
     
    user_id_bot = int(user_id_bot)
    form_id = int(form_id)
    audit_id = int(audit_id)
    univ_master_form = int(univ_master_form)
    univ_user_fld_tag = int(univ_user_fld_tag)
    
    #
    token = get_authorization(connection, user_id_bot)
    print(f"token=  {token}")
    opt_value = get_opt_value(connection, audit_id)
    print(f"opt_value = {opt_value}")
    master_audit = get_master_audit(connection, univ_master_form, opt_value)
    print(f" master_audit= {master_audit}")
    user_id = get_user_id(connection, master_audit, univ_user_fld_tag)
    print(f"user_id = {user_id}")
    ##################
    nc_status = get_nc_status(connection, audit_id, form_id)
    if nc_status != None: 
        for nc_statuss in nc_status: 
            ans_id = nc_statuss[0]     
            print("calling  update function ------")
            table_update(token, base_url, user_id, user_id_bot, audit_id, ans_id)
            print("called update function ------")