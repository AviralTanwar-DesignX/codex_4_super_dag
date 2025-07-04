from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta,time,date
from airflow.models import Variable

import requests
import subprocess     # To run shell commands (e.g., file deletion)
import pandas as pd
import numpy as np
import os
import pytz
import logging
import re

import connections_airflow as dfos_con
import airflow_library as bot_func
import connections_airflow_write as dfos_con_write

##################################################################################################################################################
##################################################################################################################################################
##################################################################################################################################################
##################################################################################################################################################
##################################################################################################################################################
##################################################################################################################################################

def fetch_excel_path(connection, audit_id, field_id):
    try:
        cur = connection.cursor()
        sql_query = ''' 
                           SELECT ans.answer, ans.created_at
                            FROM form_via_form_main_audit_answers AS ans
                            WHERE ans.fvf_main_audit_id = %s and ans.fvf_main_field_id = %s
                    '''

        cur.execute(sql_query, (audit_id, field_id))  
        result = cur.fetchall()
        return result[0][0], result[0][1]
    except Exception as e:
        print(f"Error occurred in the func => fetch_excel_path: {e}")
        raise 


def download_file(excel_path, file_path, url):
    try:
        excel_url = f"{url}uploads/form_via_form/answertype_attachment/{excel_path}"
        print("Excel Url:- ", excel_url)

        response = requests.get(excel_url)
        response.raise_for_status()

        # Save to temp file first
        temp_path = f"/tmp/{os.path.basename(file_path)}"
        with open(temp_path, "wb") as f:
            f.write(response.content)

        # Move the file using sudo
        subprocess.run(["sudo", "mkdir", "-p", os.path.dirname(file_path)], check=True)
        subprocess.run(["sudo", "mv", temp_path, file_path], check=True)

        return f"File downloaded and saved to {file_path} (via sudo)"
    
    except requests.exceptions.RequestException as e:
        return f"Failed to download file: {e}"

    except Exception as e:
        print(f"Error occurred in the func => download_file_with_sudo: {e}")
        raise


def get_module_id(connection, form_id):
    try:
        cur = connection.cursor()
        sql_query = ''' 
                            SELECT main.module_id
                            FROM form_via_form_main_forms AS main 
                            WHERE main.fvf_main_form_id = %s
                            AND main.deleted_at = "0000-00-00 00:00:00"; 
                    '''

        cur.execute(sql_query, (form_id))  
        result = cur.fetchall()
        return result[0][0]
    except Exception as e:
        print(f"Error occurred in the func => get_module_id: {e}")
        raise 


def get_section(connection, form_id):

    """
        Fetches section IDs and section names for a given form ID from the database.

       Parameters:
            connection: MySQL database connection object.
            form_id (int): ID of the main form.

        Returns:
            Tuple[List[int], List[str]]: A tuple containing a list of section IDs and a list of corresponding section names.
    """
    try:
        cur = connection.cursor()
        sql_query = '''
                SELECT sec.fvf_section_id, sec.fvf_section_name
                FROM form_via_form_main_sections AS sec
                WHERE sec.fvf_main_form_id = %s
                AND sec.deleted_at = "0000-00-00 00:00:00";
                        '''

        cur.execute(sql_query,(form_id)) 
        result = cur.fetchall()
        section_id = [row[0] for row in result]
        section_name = [row[1] for row in result]

        return section_id, section_name
    except Exception as e:
        print(f"Error occurred in the func => get_section: {e}")
        raise 


def fields(connection, form_id):

    """
        Fetches all fields for a given form along with their global tag, type, and section association.

        Parameters:
            connection: MySQL database connection object.
            form_id (int): ID of the main form.

        Returns:
            Dict[int, Dict]: A dictionary where keys are field IDs and values are metadata 
                            including field name, type, global tag, and section ID.
    """
    try:
        cur = connection.cursor()
        sql_query = '''
            SELECT fiel.fvf_main_field_name, 
                   fiel.fvf_main_field_id, 
                   fiel.fvf_main_field_type,  
                   gt.global_tag_name,
                   fiel.fvf_section_id  -- Added section_id here
            FROM form_via_form_main_fields AS fiel
            JOIN global_tags AS gt ON gt.global_tag_id = fiel.global_tag_id
            WHERE fiel.fvf_main_form_id = %s
              AND fiel.deleted_at = "0000-00-00 00:00:00";
        '''

        cur.execute(sql_query, (form_id))
        result = cur.fetchall()

        data_dict = {
            row[1]: {
                'field_name': row[0],
                'fvf_main_field_type': row[2],
                'global_tag': row[3],
                'section_id': row[4]  
            } for row in result
        }
        
        return data_dict
    except Exception as e:
        print(f"Error occurred in the func => fields: {e}")
        raise
 
 
def create_section_dict(connection, submission_form_id):
    """
        Combines section and field metadata into a structured dictionary that organizes fields under their respective sections.

        Parameters:
            connection: MySQL database connection object.
            submission_form_id (int): ID of the form being processed for submission.

        Returns:
            Dict[str, Dict]: A nested dictionary where each key is a section ID (as string) and each value contains
                            section details and its associated fields.
    """
    try:

        # Step 1: Get all section IDs and names for the form
        section_id, section_name = get_section(connection, submission_form_id)
        print("Section ids:- ", section_id, " Section names:- ", section_name)
        
        # Step 2: Get all fields belonging to the form
        data_dict = fields(connection, submission_form_id)
        print("Data Dict:- ", data_dict)
        
        final_dict = {}
        
        # Step 3: Construct the section-wise dictionary
        for idx, sec_id in enumerate(section_id):
            section = {
                'section_id': sec_id,
                'section_name': section_name[idx],
                'fields': {}
            }

            # Add all fields that belong to this section
            for field_id, field_info in data_dict.items():
                if field_info.get('section_id') == sec_id:
                    section['fields'][field_id] = {
                        'field_name': field_info['field_name'],
                        'global_tag': field_info['global_tag'],
                        'field_type': field_info['fvf_main_field_type']
                    }
            
            # Use section ID as key (converted to string for consistency)
            final_dict[f"{sec_id}"] = section
        
        return final_dict
    except Exception as e:
        print("Error occured int he function create_section_dict:-", e)
        raise

        
def fetch_answers(connection, audit_id, data_dict):
    try:
        cur = connection.cursor()
        sql_query = '''
            SELECT ans.answer, ff.global_tag_id
            FROM form_via_form_main_audit_answers AS ans
            JOIN form_via_form_main_fields AS ff ON ff.fvf_main_field_id = ans.fvf_main_field_id
            WHERE ans.fvf_main_audit_id = %s
            AND ans.deleted_at = "0000-00-00 00:00:00";
        '''

        cur.execute(sql_query,(audit_id))
        result = cur.fetchall()

        answers_dict = {row[1]: row[0] for row in result}

        for field_id, field_data in data_dict.items():
            global_tag = field_data.get('global_tag')
            
            if global_tag in answers_dict:
                field_data['answer'] = answers_dict[global_tag]  

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

        cur.execute(sql_query, (user_id))  
        result = cur.fetchall()
        return result[0][0], result[0][1]
        
    except Exception as e:
        print(f"Error occurred in the func => users_func: {e}")
        raise  


def create_directory_for_file(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        try:
            subprocess.run(['sudo', 'mkdir', '-p', directory], check=True)
            print(f"Directory created: {directory}")
        except subprocess.CalledProcessError as e:
            print(f"Failed to create directory with sudo: {e}")
    else:
        print("Directory already exists.")
    

def delete_file(file_path):
    try:
        logger = logging.getLogger(__name__)

        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f"File {file_path} successfully deleted.")
            except PermissionError:
                logger.info(f"Permission denied for {file_path}, trying with sudo...")
                subprocess.run(['sudo', 'rm', '-f', file_path], check=True)
                logger.info(f"File {file_path} successfully deleted using sudo.")
        else:
            logger.warning(f"File {file_path} does not exist.")
    except Exception as e:
        logger.error(f"Error occurred while deleting file: {e}")
        raise

def find_header_row(file_path, ext, expected_columns=None):
    expected_columns = expected_columns or ['Division', 'Dept', 'Description', 'Sub-Dept']

    if ext == '.csv':
        preview_df = pd.read_csv(file_path, nrows=10, header=None)
    else:
        preview_df = pd.read_excel(file_path, engine='openpyxl', nrows=10, header=None)

    for i in range(len(preview_df)):
        row_values = preview_df.iloc[i].astype(str).str.strip().tolist()
        match_count = sum(any(exp.lower() in val.lower() for val in row_values) for exp in expected_columns)
        if match_count >= len(expected_columns) * 0.6:  # 60% match threshold
            return i  # this is likely the header row

    return 0  # fallback to first row



def update_answers_api(audit_id, form_id, field_id, answer, base_url):
    try:

        data = {
            "audit_id": f"{audit_id}",
            "form_id": f"{form_id}",
            "field_id": f"{field_id}",
            "answer": f"{answer}"
        }
 
        print("data:- ", data)

        url = base_url +'api/v3/formAnswerUpdate'
        print("URL:- ", url)

        response = requests.post(url, data=data)
 
        print("Response Text:", response.text)
        # Check the response status code
        if response.status_code == 200:
            print("Data posted successfully")
        else:
            print(response.status_code)
            print("Failed to post data")
        return response
    except Exception as e:
        print(f"Error occured in the func => update_answers_api: {e}")
        raise


############

# **kwargs allows you to pass a variable number of keyword arguments to a function. 
# These arguments are collected into a dictionary where the keys are the argument names and the values are the corresponding values passed.
# This is useful when you want to handle named arguments dynamically, without knowing them beforehand.
def main_code_table_insertion_based_on_excel_submitted_in_a_form(**kwargs):   

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------
# Extracting the values sent from the config.... DO NOT CHANGE THIS AT ALL

    # Extract values from the config
    audit_id =  kwargs['audit_id']                              
    form_id = kwargs['form_id'] 
    url = kwargs['url'] 
    table_name = kwargs['table_name']
    field_id = kwargs['question_id_for_excel']

    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Print/log key variable values for debugging and auditing purposes
    # Helps trace issues if something fails during execution
    
    print(f"::group:: PARAMS =====")

    print("FORM ID:- ",form_id)
    print("AUDIT ID:- ", audit_id)

    print("::endgroup::")

    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Setting up email subject and database connection
    # Note: Push notification logic is not implemented at this point

    connection = dfos_con.connection_for_all_master(url) 

    engine = dfos_con_write.connection_dfos_itc_live_write_engine()
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------MAIN CODE'S LOGIC STARTS FROM BELOW------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    print("-------------------------------------------------------------------------------------------------------------------")
    print("-------------------------------------------------------------------------------------------------------------------")

# -----------------------
# Step 1: Get Excel path
# -----------------------
# Fetches the Excel file name/path associated with the given audit_id from the database
    excel_path,created_at = fetch_excel_path(connection, audit_id, field_id)
    print("created_at:- ", created_at)

# -----------------------
# Step 2: Construct full file path
# -----------------------
# This is where the Excel will be stored after download - 
# file_path = f"//home/airflowdfos/attachments/{dag_id}/{excel_path}"
    file_path = f"//home/airflowdfos/attachments/CompanyName_ProjectName_GlobalStandardBot_main_json_parser/{excel_path}"

# -----------------------
# Step 3: Ensure directory exists
# -----------------------
# Creates the directory for the file path if it doesn't already exist
    create_directory_for_file(file_path)

# -----------------------
# Step 4: Download the file
# -----------------------
# Downloads the Excel from the Digital Twin/DFOS server and saves it locally
    response = download_file(excel_path, file_path, url)
    print("response:- ", response)

# -----------------------
# Step 5: Determine file extension
# -----------------------
# Extracts the file extension (e.g., .csv, .xlsx, etc.) and converts it to lowercase
    ext = os.path.splitext(file_path)[1].lower()

# -----------------------
# Step 6: Read the Excel/CSV file into a DataFrame
# -----------------------
# Loads the file into a pandas DataFrame depending on the extension
    # Detect format
# === Usage ===
    if ext == '.csv':
        header_row = find_header_row(file_path, ext)
        df = pd.read_csv(file_path, header=header_row)
    elif ext in ['.xlsx', '.xls']:
        header_row = find_header_row(file_path, ext)
        df = pd.read_excel(file_path, engine='openpyxl', header=header_row)
    else:
        raise ValueError("Unsupported file format")

    print("✅ Header row dynamically detected at index:", header_row)


# Step 7: Clean column names in DataFrame
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[’']", '', regex=True)             # Remove smart/regular apostrophes
        .str.replace(r"[()]", '', regex=True)             # Remove parentheses
        .str.replace(r"/", '_', regex=True)               # Replace slashes with underscore
        .str.replace(r"[^a-zA-Z0-9_]+", '_', regex=True)  # Replace non-alphanum (except _) with underscore
        .str.replace(r"_+", '_', regex=True)              # Collapse multiple underscores into one
        .str.strip('_')                                   # Remove leading/trailing underscores
    )


# Step 8: Add timestamp columns
# Get current UTC time from Airflow and convert to IST


    df['created_at'] = created_at
    df['updated_at'] = pd.NaT
    df['deleted_at'] = pd.NaT

    # Finally, insert into table
    df.to_sql(table_name, con=engine, index=False, if_exists='append')
    print("✅ Data inserted into MySQL table: leading_indicators_for_ehs")

# -----------------------
# Step 16: Cleanup - Delete the downloaded Excel after use
# -----------------------
    delete_file(file_path)  

# -----------------------
# Step 17: Close the database connection
# -----------------------
# Always close the database connection to release resources
    connection.close()
    engine.dispose()

# -----------------------
# Step 18: Final return value for the task
# -----------------------
    return "DesignX"