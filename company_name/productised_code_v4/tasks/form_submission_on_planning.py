from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta,time,date
from airflow.models import Variable

import connections_airflow as dfos_con
import connections_airflow_write as dfos_con_write

import requests
import json
import pymysql

def api_fetch_2(audit_id, user_id_bot, authorization, url):
    api_endpoint = f"{url}api/v2/formviaform/secondary/ifblock-status-new"
    # api_key = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiODM2NCIsImVtYWlsIjoiZGVlcC5zaGV0aEBkZXNpZ254LmluIiwidGltZXN0YW1wIjoxNzE5ODM2NDM0LCJjb21wYW55X2lkIjoiMTc3In0.EmoNVxrtzcY_CaGMx_T-LbKo24IFUKJWkoKig_UKFm4"
    api_key = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiODM2NCIsImVtYWlsIjoiZGVlcC5zaGV0aEBkZXNpZ254LmluIiwidGltZXN0YW1wIjoxNzE5ODM2NDM0LCJjb21wYW55X2lkIjoiMTc3In0.EmoNVxrtzcY_CaGMx_T-LbKo24IFUKJWkoKig_UKFm4"

    json_payload = {'user_id': {user_id_bot},
    'fvf_main_audit_id': {audit_id}}
    files=[

    ]
    headers = {
    # "Content-Type": "application/json; charset=utf-8",
    'platform_type' : str(4),
    'Authorization': f'{authorization}',
    # 'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiOTE3MSIsImVtYWlsIjoiYm90LnVzZXJAZGVzaWdueC5pbiIsInRpbWVzdGFtcCI6MTczMDk1Njk1OCwiY29tcGFueV9pZCI6IjEwMSJ9.rzkCcYlGeq5J8DIogPlb3ZlVuIakiqnlshUGvN6F4g4',
    'Cookie': 'ci_session=o5sj1fk1pujleqfua4u3saerd4totc2h'
    }
    response = requests.post(api_endpoint, headers=headers, data=json_payload)

    # Check the response status code
    if response.status_code == 200:
        print("Request was successful")
        print("Response data:", response.json())
    else:
        print(f"Request failed with status code {response.status_code}")
        print("Response text:", response.text)
 # function for calling the api


def api_fetch_1(json_payload, user_id_bot, authorization, url):
    api_endpoint = f"{url}api/v3/formviaform/mainform/audit/insert/repeatsection"
    print(api_endpoint)
    headers = {
        'platform_type' : str(4),
        'Authorization': f'{authorization}',
        'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
    }

    print("Sending payload:", json.dumps(json_payload, indent=4))

    response = requests.post(api_endpoint, headers=headers, data=json_payload)

    if response.status_code == 200:
        print("Fetch 1 = Request was successful")
        print("Response data:", response.json())
        data = response.json()
        print(data,"satyam")

        # Safely try to access the fvf_main_audit_id key
        fvf_main_audit_id = data.get('Root', {}).get('fvf_main_audit_id')

        if fvf_main_audit_id:
            print(fvf_main_audit_id)
            api_fetch_2(fvf_main_audit_id, user_id_bot,authorization, url)
            print(api_fetch_2,"api_fetch_2 called")
            return fvf_main_audit_id
        else:
            print("Key 'fvf_main_audit_id' not found in the response.")

    else:
        print(f"Request failed with status code {response.status_code}")
        print("Response text:", response.text)




def get_info_for_submission(connection,form_id):
    try:
        cur = connection.cursor()
        sql_query = f'''
SELECT sec.fvf_section_id,sec.fvf_section_name,main.module_id
from  form_via_form_main_forms AS main
LEFT JOIN form_via_form_main_sections AS sec ON main.fvf_main_form_id = sec.fvf_main_form_id
WHERE main.fvf_main_form_id = {form_id} AND sec.deleted_at =  0 AND main.deleted_at = 0;                     
                        '''

        cur.execute(sql_query)  # Passing audit_id as a parameter
        result = cur.fetchall()
        print(result)
        if result:
            return result[0][0],result[0][1],result[0][2]
        else:
            return []
    except Exception as e:
        print(f"Error occurred in the func => get_info_for_submission: {e}")
        raise

def get_info_for_submission_dict(connection, form_id, len):
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cur:
            sql_query = f'''              
                    SELECT 
                    main.fvf_main_field_id AS main_field_id,
                    main.fvf_main_field_type AS main_field_type,
                    main.fvf_main_field_name AS main_field_name
                FROM form_via_form_main_fields AS main 
                WHERE main.fvf_main_form_id = %s
                  AND main.deleted_at = '0000-00-00 00:00:00' 
						ORDER BY main.fvf_main_field_id ASC
                        limit {len};
            '''
            cur.execute(sql_query, (form_id,))
            result = cur.fetchall()

            if not result:
                raise ValueError(f"No field data found for form_id = {form_id}")

            return result

    except Exception as e:
        print(f"Error occurred in get_info_for_submission_dict: {e}")
        raise

def get_auth_for_user(connection,bot_user):
    try:
        cur = connection.cursor()
        sql_query = f'''SELECT u.Authorization
FROM users AS u
WHERE u.user_id = {bot_user};                        
                        '''

        cur.execute(sql_query)  # Passing audit_id as a parameter
        result = cur.fetchall()
        return result[0][0] if result else []
    except Exception as e:
        print(f"Error occurred in the func => get_auth_for_user: {e}")
        raise


def answer_submission_json_dynamic(section_id, section_name, form_id, bot_user_id, module_id, field_data_list):
    try:
        questions = []
        for field in field_data_list:
            # Convert everything to string before inserting
            answer = str(field["answer"]) if field["answer"] is not None else ""

            option_id = ''
            machine_option_id = ''

            # if field["main_field_id"] == 361089:
            #     option_id = model_id

            # if field["main_field_id"] == 361088:
            #     option_id = location_id   

            
            question = {
                "fvf_main_field_name": str(field["main_field_name"]),
                "is_nc_hold": "0",
                "fvf_main_ans_id": "",
                "task_target_date": "",
                "answer": answer,
                "is_na": "0",
                "nc_remark": "",
                "fvf_main_field_option_id": f"{option_id}",
                "is_nc_marked": "0",
                "is_rnc": "0",
                "is_improvement": "0",
                "answer_remark": "",
                "audit_temp_image": "",
                "answer_number_value": "",
                "fvf_qrscan_answer_id": "",
                "fvf_main_field_option_value": "",
                "fvf_main_field_id": str(field["main_field_id"]),
                "workflow_userids": "",
                "task_responsibility_id": "",
                "location_latlong": "",
                "workflow_type": 0,
                "option_question_answer": [],
                "answer_attachment": "",
                "workflow_ncform_id": "",
                "is_value": "",
                "fvf_main_field_type": str(field["main_field_type"])
            }
            questions.append(question)

        question_array = [
            {
                "question_array": [questions],
                "fvf_section_name": section_name,
                "fvf_section_id": section_id
            }
        ]

        print("Question Array:")
        print(json.dumps(question_array, indent=4))

        json_payload = {
            'plant_id': '',
            'fvf_schedule_id': ' ',
            'form_unique_number': ' ',
            'linked_mainaudit_id': '',
            'fvf_section_id': '',
            'is_secondary_audit': '0',
            'subresource_question': ' ',
            'sku_id': ' ',
            'is_process_termination': ' 0',
            'resource_question': ' ',
            'alarm_type': ' ',
            'audit_duration': '0.5',
            'fvf_main_audit_id': ' ',
            'form_material': ' ',
            'subresource_id': ' ',
            'area_id': '',
            'site_id': '',
            'form_answer_json': json.dumps(question_array),
            'sku_question': ' ',
            'alarm_notification_id': ' ',
            'reference_answer_id': '',
            'proxy_date': ' ',
            'proxydate_question': ' ',
            'nc_id': ' ',
            'fvf_main_form_id': form_id,
            'line_id': '',
            'fvf_qrcode_id': ' ',
            'module_id': module_id,
            'resource_id': '',
            'user_id': bot_user_id,
            'form_resource': ''
        }

        print("\nFinal JSON Payload:")
        print(json.dumps(json_payload, indent=4))

        return json_payload

    except Exception as e:
        print(f"Error occurred: {e}")
        raise


def get_latest_production_plan(connection, location_id, columns):
    """
    Fetch the latest production planning entry for the given location_id.
    Returns a flat list (1D array) of the values in the row.

    Args:
        connection (pymysql.connections.Connection): Active DB connection
        location_id (int): Location ID to filter the data
        columns (list): List of column names to fetch

    Returns:
        list: A flat list of values representing the latest production plan.

    Raises:
        ValueError: If no planning data is found for the given location.
    """
    if not columns:
        raise ValueError("Column list is empty.")

    select_clause = ",\n    ".join(columns)

    try:
        with connection.cursor() as cur:
            sql_query = f'''
                SELECT 
                    {select_clause}
                FROM tbl_production_planning AS t
                
                WHERE t.id = %s
                ORDER BY t.id DESC
                LIMIT 1
            '''
            print("Executing SQL Query:\n", sql_query)
            cur.execute(sql_query, (location_id,))
            result = cur.fetchone()

            if not result:
                raise ValueError(f"No production plan found for location_id = {location_id}")

            return list(result)

    except Exception as e:
        print(f"Error in get_latest_production_plan: {e}")
        raise



def get_latest_production_plan_1(connection, location_id):
    """
    Fetch the latest production planning entry for the given location_id.
    Returns a flat list (1D array) of the values in the row.

    Args:
        connection (pymysql.connections.Connection): Active DB connection
        location_id (int): Location ID to filter the data

    Returns:
        list: A flat list of values representing the latest production plan.

    Raises:
        ValueError: If no planning data is found for the given location.
    """
    try:
        with connection.cursor() as cur:
            sql_query = '''
                SELECT 
                    
                    t.location_id
                    
                FROM tbl_production_planning AS t
                LEFT JOIN locations AS l ON t.location_id = l.location_id
                LEFT JOIN tbl_mould_planning AS m ON m.mould_id = t.mould_id
                WHERE t.id = %s
                ORDER BY t.id DESC
                LIMIT 1;
            '''
            cur.execute(sql_query, (location_id,))
            result = cur.fetchall()

            if not result:
                raise ValueError(f"No production plan found for location_id = {location_id}")

            print(list(result))
            return result[0][0]

    except Exception as e:
        print(f"Error in get_latest_production_plan: {e}")
        raise        

# Abe inhone hit kr rkhi hai apne pe.. ss provide 
def main_code_form_submission_on_planning(**kwargs):
    
    id = kwargs.get('id')
    form_ids = kwargs.get('submission_form_id')
    bot_user_id = kwargs.get('bot_user_id')
    machine_ids = kwargs.get('machine_ids')
    field_mapping =   kwargs.get('mapping')
    url  = kwargs.get('url')

    field_mapping_list = field_mapping
    machine_ids_list = [int(x.strip()) for x in machine_ids.split(",")]
    form_ids_list = [int(x.strip()) for x in form_ids.split(",")]
    
    connection = dfos_con.connection_for_all_master(url)

    location_id = get_latest_production_plan_1(connection,id)    

    if location_id in machine_ids_list:

        machine_index =  machine_ids_list.index(location_id)
        form_id_index, field_mapped_index = machine_index, machine_index
        form_id = form_ids_list[form_id_index]
        mapped_field = field_mapping_list[field_mapped_index]
        mapped_field = dict(sorted(mapped_field.items()))

        main_field_dict=  get_info_for_submission_dict(connection,form_id, len(mapped_field))
        section_id,section_name,module_id = get_info_for_submission(connection,form_id)
        columns = list(mapped_field.values())
        print(columns)

        answers = get_latest_production_plan(connection,id, columns)
       
        print(answers)

        for i, field in enumerate(main_field_dict[:len(answers)]):
            field["answer"] = answers[i]

        print(main_field_dict)
            
        # print(main_field_dict, "\n", section_id, section_name)
        answer_json = answer_submission_json_dynamic(section_id,section_name,form_id,bot_user_id,module_id,main_field_dict)
        # print(answer_json)
        bot_auth = get_auth_for_user(connection,bot_user_id)
        print(bot_auth)
        
        api_fetch_1(answer_json,bot_user_id,bot_auth, url)

        return "DesignX"
