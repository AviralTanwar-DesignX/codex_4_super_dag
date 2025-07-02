from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta,time,date
from airflow.models import Variable

import requests
import json
import pandas as pd
import copy
import numpy as np

import connections_airflow as dfos_con
import airflow_library as bot_func
import connections_airflow_write as dfos_con_write


def api_fetch_2(audit_id, user_id_bot, authorization, url):
    # url = dfos_con_write.fetch_url_from_env_var("staging")
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
    headers = {
        'platform_type' : str(4),
        'Authorization': f'{authorization}',
        'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
    }

    # print("Sending payload:", json.dumps(json_payload, indent=4))

    response = requests.post(api_endpoint, headers=headers, data=json_payload)

    if response.status_code == 200:
        print("Fetch 1 = Request was successful")
        print("Response data:", response.json())
        data = response.json()

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
        


def generate_dynamic_json(full_section_data):
    try:
        final_question_array = []

        for section_key, section_info in full_section_data.items():
            section_id = section_info['section_id']  # fixed
            section_name = section_info['section_name']  # fixed
            fields_dict = section_info.get('fields', {})
            field_ids = list(fields_dict.keys())

            for i in range(0, len(field_ids), 3):  # Group questions by 3
                question_set = []

                for j in range(3):
                    if i + j < len(field_ids):
                        field_id = field_ids[i + j]
                        field_data = fields_dict[field_id]

                        question = {
                            "fvf_main_field_name": field_data['field_name'],
                            "is_nc_hold": "0",
                            "fvf_main_ans_id": "",
                            "task_target_date": "",
                            "answer": field_data.get("answer", ""),
                            "is_na": "0",
                            "nc_remark": "",
                            "fvf_main_field_option_id": field_data.get("sku_id", ""),
                            "is_nc_marked": "0",
                            "is_rnc": "0",
                            "is_improvement": "0",
                            "answer_remark": "",
                            "audit_temp_image": "",
                            "answer_number_value": "",
                            "fvf_qrscan_answer_id": "",
                            "fvf_main_field_option_value": "",
                            "fvf_main_field_id": str(field_id),
                            "workflow_userids": "",
                            "task_responsibility_id": "",
                            "location_latlong": "",
                            "workflow_type": 0,
                            "option_question_answer": [],
                            "answer_attachment": "",
                            "workflow_ncform_id": "",
                            "is_value": "",
                            "fvf_main_field_type": field_data['field_type']  # fixed key here too
                        }

                        question_set.append(question)

                final_question_array.append({
                    "question_array": [question_set],
                    "fvf_section_name": section_name,
                    "fvf_section_id": str(section_id)
                })

        return final_question_array

    except Exception as e:
        print(f"Error in generate_dynamic_json: {e}")
        raise

def convert(o):
    if isinstance(o, (np.int64, np.int32)):
        return int(o)
    if isinstance(o, (np.float64, np.float32)):
        return float(o)
    if isinstance(o, (np.bool_)):
        return bool(o)
    return str(o)

def create_full_json(question_array, submission_module_id, bot_user_id,site_id, plant_id, area_id, line_id, resource_id, form_id):
    try:
        json_payload = {
    'plant_id': f'{plant_id}',
    'fvf_schedule_id': ' ',
    'form_unique_number': ' ',
    'linked_mainaudit_id': '0',
    'fvf_section_id': '',
    'is_secondary_audit': '',
    'subresource_question': ' ',
    'sku_id': ' ',
    'is_process_termination': ' 0',
    'resource_question': ' ',
    'alarm_type': ' ',
    'audit_duration': '0.5',
    'fvf_main_audit_id': ' ',
    'form_material': ' ',
    'subresource_id': ' ',
    'area_id': f'{area_id}',
    'site_id': f'{site_id}',
    'form_answer_json': '',
    'sku_question': ' ',
    'alarm_notification_id': ' ',
    'reference_answer_id': f'',
    'proxy_date': ' ',
    'proxydate_question': ' ',
    'nc_id': ' ',
    'fvf_main_form_id': f'{form_id}',
    'line_id': f'{line_id}',
    'fvf_qrcode_id': ' ',
    'module_id': f'{submission_module_id}', 
    'resource_id': f'{resource_id}',
    'user_id': f'{bot_user_id}',
    'form_resource': '',
    'form_answer_json': json.dumps(question_array, default=convert)
    
}
        return json_payload
    except Exception as e:
        print(f"Error occurred in the function create_full_json: {e}")
        raise  




##


def fetch_tag_columns_by_timestamp(connection, target_timestamp,machine_name):
    try:
        with connection.cursor() as cursor:
            # Fetch one row to get column names
            cursor.execute(f"SHOW COLUMNS FROM {machine_name}")
            all_columns = [col[0] for col in cursor.fetchall()]  # Access tuple element by index
            
            # Filter columns starting with 'tag' (case-insensitive)
            tag_columns = [col for col in all_columns if col.lower().startswith("tag")]

            if not tag_columns:
                print("No tag columns found.")
                return []

            # Build dynamic SELECT query
            columns_str = ", ".join(f"`{col}`" for col in tag_columns)
            print("COUMN STR:- ", columns_str)
            print("MAchine Name;- ", machine_name)
            print("Created at :- ", target_timestamp)
            
            query = f'''
                    SELECT {columns_str}
                    FROM `{machine_name}`
                    WHERE created_at = %s
'''
            cursor.execute(query, (target_timestamp,))


            rows = cursor.fetchall()

            if not rows:
                print("No data found for the specified timestamp.")
                return []

            # Build dataframe-like structure
            dataframe = [tag_columns]
            for row in rows:
                dataframe.append(list(row))  # Each row is a tuple

            return dataframe
    except Exception as e:
        print(f"Error occurred in the func => fetch_tag_columns_by_timestamp: {e}")
        raise



def get_module_id(connection, form_id):
    try:
        cur = connection.cursor()
        sql_query = f''' 
                            SELECT main.module_id
                            FROM form_via_form_main_forms AS main 
                            WHERE main.fvf_main_form_id = {form_id}
                            AND main.deleted_at = "0000-00-00 00:00:00"; 
                    '''

        cur.execute(sql_query)  # Passing form_id as a parameter
        result = cur.fetchall()
        return result[0][0]
    except Exception as e:
        print(f"Error occurred in the func => get_module_id: {e}")
        raise 


def get_section(connection, form_id):
    try:
        cur = connection.cursor()
        sql_query = f'''
                SELECT sec.fvf_section_id, sec.fvf_section_name
                FROM form_via_form_main_sections AS sec
                WHERE sec.fvf_main_form_id = {form_id}
                AND sec.deleted_at = "0000-00-00 00:00:00";
                        '''

        cur.execute(sql_query)  # Passing audit_id as a parameter
        result = cur.fetchall()
        section_id = [row[0] for row in result]
        section_name = [row[1] for row in result]

        return section_id, section_name
    except Exception as e:
        print(f"Error occurred in the func => get_section: {e}")
        raise 


def fields(connection, form_id):
    try:
        cur = connection.cursor()
        sql_query = f'''
            SELECT fiel.fvf_main_field_name, 
                   fiel.fvf_main_field_id, 
                   fiel.fvf_main_field_type,  
                   gt.global_tag_name,
                   fiel.fvf_section_id  -- Added section_id here
            FROM form_via_form_main_fields AS fiel
            JOIN global_tags AS gt ON gt.global_tag_id = fiel.global_tag_id
            WHERE fiel.fvf_main_form_id = {form_id}
              AND fiel.deleted_at = "0000-00-00 00:00:00";
        '''

        cur.execute(sql_query)
        result = cur.fetchall()

        data_dict = {
            row[1]: {
                'field_name': row[0],
                'fvf_main_field_type': row[2],
                'global_tag': row[3],
                'section_id': row[4]  # Include section_id in the dictionary
            } for row in result
        }
        
        return data_dict
    except Exception as e:
        print(f"Error occurred in the func => fields: {e}")
        raise
 
 
def create_section_dict(connection, submission_form_id):
    # Get sections
    section_id, section_name = get_section(connection, submission_form_id)
    print("Section ids:- ", section_id, " Section names:- ", section_name)
    
    # Get fields data
    data_dict = fields(connection, submission_form_id)
    print("Data Dict:- ", data_dict)
    
    # Create the final dictionary
    final_dict = {}
    
    # Loop through each section
    for idx, sec_id in enumerate(section_id):
        section = {
            'section_id': sec_id,
            'section_name': section_name[idx],
            'fields': {}
        }

        # Loop through the fields and attach to the correct section
        for field_id, field_info in data_dict.items():
            if field_info.get('section_id') == sec_id:
                section['fields'][field_id] = {
                    'field_name': field_info['field_name'],
                    'global_tag': field_info['global_tag'],
                    'field_type': field_info['fvf_main_field_type']
                }

        # Add section to final_dict using its ID
        final_dict[f"{sec_id}"] = section
    
    return final_dict


        
def fetch_answers(connection, audit_id, data_dict):
    try:
        cur = connection.cursor()
        sql_query = f'''
            SELECT ans.answer, ff.global_tag_id
            FROM form_via_form_main_audit_answers AS ans
            JOIN form_via_form_main_fields AS ff ON ff.fvf_main_field_id = ans.fvf_main_field_id
            WHERE ans.fvf_main_audit_id = {audit_id}
            AND ans.deleted_at = "0000-00-00 00:00:00";
        '''

        cur.execute(sql_query)
        result = cur.fetchall()

        # Create a dictionary from the query results with global_tag_id as keys and answers as values
        answers_dict = {row[1]: row[0] for row in result}

        # Now, loop through data_dict and add the corresponding answer to each field where the global_tag matches
        for field_id, field_data in data_dict.items():
            global_tag = field_data.get('global_tag')
            
            if global_tag in answers_dict:
                field_data['answer'] = answers_dict[global_tag]  # Append the answer to the field's data

        return data_dict
    
    except Exception as e:
        print(f"Error occurred while fetching answers: {e}")


def users_func(connection, user_id):
    try:
        cur = connection.cursor()

        sql_query = '''
                    SELECT us.Authorization, us.email
                    FROM users AS us
                    WHERE us.user_id = %s
                    AND us.deleted_at = "0000-00-00 00:00:00";
                        '''

        cur.execute(sql_query, (user_id))  # Passing audit_id as a parameter
        result = cur.fetchall()
        return result[0][0], result[0][1]
        
    except Exception as e:
        print(f"Error occurred in the func => users_func: {e}")
        raise  


def extract_data(connection, data_dict, audit_id):
    try:
        cur = connection.cursor()  
        # Define the desired order based on global_tag values
        desired_order = [245,246,247,248,249,250]

        # Filter out 'Invoice Number' and reorder the answers based on the global_tag order
        ordered_answers = [value['answer'] for key, value in data_dict.items() if value['global_tag'] in desired_order]

        # Ensure there are exactly 5 answers and add audit_id as the last element
        # answer_tuple = [(
        #     ordered_answers[4],            # 'Quantity Received'
        #     ordered_answers[2],            # 'Lot No'
        #     ordered_answers[3],            # 'Supplier Name'
        #     ordered_answers[0],            # 'SKU'
        #     ordered_answers[1],            # 'Delivery Date'
        #     audit_id                       # Last element is the audit_id
        # )]
        
        answer_tuple = [(
            ordered_answers[3],            # 'raw_material'
            ordered_answers[2],            # 'date_of_receipt'
            ordered_answers[1],            # 'invoice_no'
            ordered_answers[4],            # 'unit'
            ordered_answers[0],            # 'Supplier_name'
            ordered_answers[5],            # 'quantity'
            audit_id         ,              # 'material_audit_id'
            ordered_answers[5],            # 'final_stock_balance'
        )]

        print("answer_tuple = ",answer_tuple)  # Output the list of tuple

        return answer_tuple
    except Exception as e:
        print(f"Error occurred in the func => extract_data: {e}")
        raise

############

# **kwargs allows you to pass a variable number of keyword arguments to a function. 
# These arguments are collected into a dictionary where the keys are the argument names and the values are the corresponding values passed.
# This is useful when you want to handle named arguments dynamically, without knowing them beforehand.
def main_code(**kwargs):   

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------
# Extracting the values sent from the config.... DO NOT CHANGE THIS AT ALL

    # Extract values from the config
    id = kwargs('id')
    execution_date = kwargs['execution_date']
    submission_form_id = kwargs('submission_form_id') 
    bot_user_id = kwargs('bot_user_id') 
    url = kwargs('url')


    # id = 239
    # execution_date = '2025-05-23 09:15:25+00:00'
    # execution_date = datetime.fromisoformat(execution_date.replace('Z', '+00:00'))
    # submission_form_id =69
    # bot_user_id = 314
    # url = 'https://web.dfos.co/iljin/'


    site_id = 0
    plant_id = 0
    area_id = 0
    line_id = 0 
    resource_id = 0 
    
    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Print/log key variable values for debugging and auditing purposes
    # Helps trace issues if something fails during execution

    print(f"::group:: PARAMS =====")

    print("Machine id:- ", id)
    print("Execution Date/ Created At:-  ", execution_date)
    print("Form Id which will be submitted:- ", submission_form_id)
    print("Bot User ID:- ", bot_user_id)
    print("URL:- ", url)

    print("::endgroup::")


    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Setting up email subject and database connection
    # Note: Push notification logic is not implemented at this point

    connection = dfos_con.connection_for_all_replica(url)  
    # Read-only DB connection — uses the correct replica based on the given URL

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------
 
    print("-------------------------------------------------------------------------------------------------------------------")

    print("URL:- ", url)

    # Main Logic --
    current_time_ist = execution_date + timedelta(hours=5, minutes = 30)
    print("Created At:- ", current_time_ist)

    plc = 'plc_floattable_'+str(id)
    data = fetch_tag_columns_by_timestamp(connection, current_time_ist, plc)
    headers = data[0]
    rows = data[1:]

    df = pd.DataFrame(rows, columns=headers)

    module_id = get_module_id(connection, submission_form_id)
    print(f"module_id = {module_id}")

    final_dict = create_section_dict(connection, submission_form_id)
    print("Final Dict:- ", final_dict)

    authorization, email_next = users_func(connection, bot_user_id)
    print("authorization:- ", authorization, "email_next:- ", email_next)

    for index, row in df.iterrows():
        updated_dict = copy.deepcopy(final_dict)  # Deepcopy so original doesn't get modified

        for section_key, section_data in updated_dict.items():
            fields = section_data.get('fields', {})

            for field_id, field_meta in fields.items():
                global_tag = field_meta.get('global_tag')
                if global_tag in df.columns:
                    answer = row[global_tag]
                    # Add answer below fvf_main_field_type
                    field_meta['answer'] = answer

        print("-----------------------------------------------------------------------------------------------------------------------------------------")
        print(F"--------------------------------INSERTING THE FORM || Processing Row {index + 1}---------------------------------------")
        print("-----------------------------------------------------------------------------------------------------------------------------------------")
        print(f"Updated Dict for Row {index + 1}:")
        print(updated_dict)

        question_array=  generate_dynamic_json(updated_dict)
        print("question_array:- ----------------------------------------------------------------------------------------------")
        print("question_array:- ----------------------------------------------------------------------------------------------")
        print(question_array)
        print("question_array:- ----------------------------------------------------------------------------------------------")
        print("question_array:- ----------------------------------------------------------------------------------------------")
        json_payload = create_full_json(question_array, module_id, bot_user_id,site_id, plant_id, area_id, line_id, resource_id, submission_form_id)
        print("json ------------------------------------------------------------------------------------------------------------------------------------")
        print(json.dumps(json_payload, indent = 4))
        print("json ------------------------------------------------------------------------------------------------------------------------------------")
        api_fetch_1(json_payload, bot_user_id, authorization, url)
    
    return "DesignX"