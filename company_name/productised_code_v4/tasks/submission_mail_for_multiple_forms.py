#####
# --------------------------------------------------------------------------------
# DAG TASK:
#       An Excel file is uploaded via a IOT MODULE. For each row in the Excel, a form is submitted.
#       The excel is sotred in a plc_floattable machine with each data getting stored in the tags columns
# --------------------------------------------------------------------------------

# ----------------------
# PRE-REQUISITES by DFOS-DEVELOPER:
# ----------------------
# 1) DFOS Developer must map universal tags.
#    For now, global tags must match Excel's column headers i.e tag11 or tag12.
# 2) Form field types must be TEXTBOX or NUMBER.
#    Avoid Dropdowns, APIs, or any unsupported types.

# ----------------------
# HOW TO ATTACH THE BOT:
# ----------------------
#       1) BOT NAME: Any display name for DFOS
#       2) BOT URL Format: 
#           https://airflowprod.dfos.co:8080/api/v1/dags/{dag_id}/dagRuns
#               Example:  https://airflowprod.dfos.co:8080/api/v1/dags/CompanyName_ProjectName_GlobalStandardBot_excel_upload_in_iot_module/dagRuns
#       3) BOT CONF (JSON Format):
#           { "submission_form_id": "913", "bot_user_id": "236" }
#               
#               - submission_form_id: ID of the form to be submitted
#               - bot_user_id: Create bot user in Digital Twin, log in once, and change password if new

# ----------------------
# REQUIREMENTS:
# ----------------------
# 1) Master form fields should be globally tagged to match Excel headers i.e. tag11, tag12
# 2) Must have the following:
#    - Master Form ID
#    - Bot User ID
#    - File path: /home/airflowdfos/attachments/{dag_name}/yourfile.xlsx
# ----------------------

# ---------------------------------------------------------------------------
# WORKING:
#       1) This DAG receives an Excel file via form submission, downloads it, and reads its contents.
#       2) Each row in the Excel represents a form submission.
#       3) For each row:
#           - The row is mapped to fields in a target form using global tags.
#           - A dynamic JSON is generated based on the field structure.
#           - The form is submitted via API using a bot user.
#       4) After all rows are processed, the file is deleted and the DB connection is closed.
# ---------------------------------------------------------------------------

# ----------------------
# STANDARD CONVENTIONS:
# ----------------------
#       1) In each SQL query, select the primary ID column as NAME_DAG_id
#       2) Add tags at the end of the DAG script:
#           tags = ["CompanyName", "ProjectName", "Type of DAG", "OWNER"]
#       3) Set the Airflow owner to your username and include it in the tags
# --------------------------------------------------------------------------------

# -------------------------------------
# Standard Airflow and Python Libraries
# -------------------------------------

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta,time,date
from airflow.models import Variable

import requests
import json
import os
import pandas as pd
import subprocess
import logging
import copy
import numpy as np

import connections_airflow as dfos_con
import airflow_library as bot_func
import connections_airflow_write as dfos_con_write




def fetch_from_audits(connection, audit_id):
    try:
        cur = connection.cursor()
        sql_query = f'''          
                            SELECT ans.module_id as your_name_dag_id, ans.linked_mainaudit_id
                            FROM form_via_form_main_audits AS ans
                            WHERE ans.fvf_main_audit_id = {audit_id};
                            '''
            # print(sql_query)
        cur.execute(sql_query)
        result = cur.fetchall()

        return result[0][0], result[0][1]

    except Exception as e:
        print(f"Error occurred in the func => fetch_from_audits: {e}")
        raise  


def fetch_user_id(connection, audit_id):
    ''' Fetches the user_id who submitted the answers for a given audit_id '''
    try:
        cur = connection.cursor()

        sql_query = f''' 
                            SELECT ans.user_id as your_name_dag_id
                            FROM form_via_form_main_audit_answers AS ans
                            WHERE ans.fvf_main_audit_id  = {audit_id}; 
                    '''

        cur.execute(sql_query)  # Passing form_id as a parameter
        result = cur.fetchall()
        return result[0][0]
    except Exception as e:
        print(f"Error occured in the function fetch_user_id:- ", e)
        raise

def fetch_name(connection, submission_user_id):
    try:
        cur = connection.cursor()

        sql_query = f'''
                    SELECT us.firstname as your_name_dag_id, us.lastname
                    FROM users AS us
                    WHERE us.user_id = {submission_user_id}
                    AND us.deleted_at = "0000-00-00 00:00:00";
                        '''

        cur.execute(sql_query)  # Passing audit_id as a parameter
        result = cur.fetchall()
        return result[0][0], result[0][1]
    except Exception as e:
        print(f"Error occurred in the func => fetch_name: {e}")
        raise

def fetch_answer(connection, field_id, audit_id):
    try:
        cur = connection.cursor()

        questions = []
        answer = []

        for fields in field_id:
            
            if fields == "":
                break

            sql_query = f'''          
                            SELECT ans.fvf_main_field_name as your_name_dag_id, ans.answer
                            FROM form_via_form_main_audit_answers AS ans
                            WHERE ans.fvf_main_audit_id = {audit_id} AND ans.fvf_main_field_id in ({fields});
                            '''
            # print(sql_query)
            cur.execute(sql_query)
            result = cur.fetchall()

            questions = [row[0] for row in result]
            answer = [row[1] for row in result]

        sql_query = f'''          
                SELECT ans.created_at as your_name_dag_id
                FROM form_via_form_main_audit_answers AS ans
                WHERE ans.fvf_main_audit_id = {audit_id};
            '''
        # print(sql_query)
        cur.execute(sql_query)
        result = cur.fetchall()

        print(questions)
        print(answer)
        print(result[0][0])
        return questions, answer, result[0][0]

    except Exception as e:
        print(f"Error occurred in the func => fetch_answer: {e}")
        raise

def fetch_attachment(connection, attachment_field_id, audit_id, field_type):
    try:
        cur = connection.cursor()

        answer = []

        for fields in attachment_field_id:
            sql_query = f'''          
                           SELECT ans.fvf_main_field_type as your_name_dag_id, ans.answer, ans.answer_attachment, fiel.enable_attachment
                            FROM form_via_form_main_audit_answers AS ans
                            JOIN form_via_form_main_fields AS fiel ON fiel.fvf_main_field_id = ans.fvf_main_field_id
                            WHERE ans.fvf_main_audit_id = {audit_id} and ans.fvf_main_field_id in ({fields});
                            '''
            cur.execute(sql_query)
            result = cur.fetchall()

            answer = []
            sec_ans = []

            for row in result:
                if row[0] == field_type:
                    for file in row[1].split(','):
                        answer.append(file) 
                if row[3] == 1 or row[3] == 2:
                    sec_ans.append(row[2])

            return answer, sec_ans

    except Exception as e:
        print(f"Error occurred in the func => fetch_attachment: {e}")
        raise

def fetch_attachment_image(connection, attachment_field_image_id, audit_id, field_type):
    try:
        cur = connection.cursor()

        answer = []

        for fields in attachment_field_image_id:
            sql_query = f'''          
                            SELECT ans.fvf_main_field_type  as your_name_dag_id, ans.answer, ans.answer_image, fiel.image_attachment
                            FROM form_via_form_main_audit_answers AS ans
                            JOIN form_via_form_main_fields AS fiel ON fiel.fvf_main_field_id = ans.fvf_main_field_id
                            WHERE ans.fvf_main_audit_id = {audit_id} and ans.fvf_main_field_id in ({fields});
                            '''
            cur.execute(sql_query)
            result = cur.fetchall()

            answer = []

            for row in result:
                if row[0] == field_type:
                    for file in row[1].split(','):
                        answer.append(file.strip()) 
                if row[3] == 1:
                    answer.append(row[2])

            return answer

    except Exception as e:
        print(f"Error occurred in the func => fetch_attachment_image: {e}")
        raise

def users_func(connection, user_id):
    try:
        cur = connection.cursor()

        if isinstance(user_id, int):
            sql_query = f'''
                        SELECT us.email as your_name_dag_id
                        FROM users AS us
                        WHERE us.user_id = {user_id}
                        AND us.deleted_at = "0000-00-00 00:00:00";
                            '''

            cur.execute(sql_query)  # Passing audit_id as a parameter
            result = cur.fetchall()
            return result[0][0]
        elif isinstance(user_id, str):
            temp = user_id.split(",")
            email = ""
            for ele in temp:
                sql_query = f'''
                        SELECT us.email as your_name_dag_id
                        FROM users AS us
                        WHERE us.user_id = {ele}
                        AND us.deleted_at = "0000-00-00 00:00:00";
                            '''
                cur.execute(sql_query)  # Passing audit_id as a parameter
                result = cur.fetchall()
                email = email + f", {result[0][0]}"
                return email
        elif isinstance(user_id, list):
            email_list = []
            for ele in user_id:
                sql_query = '''
                    SELECT us.email
                    FROM users AS us
                    WHERE us.user_id = %s
                    AND us.deleted_at = "0000-00-00 00:00:00";
                '''
                cur.execute(sql_query, (ele,))
                result = cur.fetchone()
                if result:
                    email_list.append(result[0])
            email = ", ".join(email_list)
            return email   
    except Exception as e:
        print(f"Error occurred in the func => users_func: {e}")
        raise 



def message_type(url, form_name, first_name, last_name, date_time, questions, answers, ans_attach, ans_image, form_id_next, linked_audit, module_id, year, month, ans_attach_sec):
    try:
        message  = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Audit Notification</title>
</head>
<body style="font-family: Arial, sans-serif; color: #333; font-size: 14px; margin: 0; background-color: #f9f9f9; padding: 20px;">
    <div style="max-width: 600px; margin: auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);">
        
        <!-- Logo -->
        <div style="text-align: center; padding: 15px 0;">
            <img src="{url}assets/image_used/logo_name.png" alt="dfos-logo" width="100" style="display: block; margin: auto;">
        </div>
        
        <p style="font-size: 16px; color: #222;">Dear Sir/Ma'am,</p>
        <p style="line-height: 1.6;">
          <strong style="color: #0056b3;">{form_name}</strong>  has been submitted. Please review the details below:
        </p>

        <!-- Observation Details -->
        <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px; font-size: 14px;">
            <tr>
                <th style="padding: 10px; background-color: #f0f0f0; text-align: left; border-bottom: 1px solid #ddd;">Submitted By</th>
                <td style="padding: 10px; border-bottom: 1px solid #ddd;">{first_name} {last_name}</td>
            </tr>
            <tr>
                <th style="padding: 10px; background-color: #f0f0f0; text-align: left; border-bottom: 1px solid #ddd;">Submission Date</th>
                <td style="padding: 10px; border-bottom: 1px solid #ddd;">{date_time}</td>
            </tr>
    '''
         # Loop through questions and answers to add them dynamically
        for question, answer in zip(questions, answers):
            message += f'''
            <tr>
                <th style="padding: 10px; background-color: #f0f0f0; text-align: left; border-bottom: 1px solid #ddd;">{question}</th>
                <td style="padding: 10px; border-bottom: 1px solid #ddd;">{answer}</td>
            </tr>
            '''
        message +=f'''
        </table>

        <!-- Steps to Follow -->
        <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px; font-size: 14px;">
            <thead>
                <tr>
                    <th style="padding: 10px; background-color: #f0f0f0; border-bottom: 2px solid #ddd; text-align: left;">SNo.</th>
                    <th style="padding: 10px; background-color: #f0f0f0; border-bottom: 2px solid #ddd; text-align: left;">Details</th>
                    <th style="padding: 10px; background-color: #f0f0f0; border-bottom: 2px solid #ddd; text-align: center;">Button</th>
                </tr>
            </thead>
        <tbody>
            <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #ddd;">1</td>
                    <td style="padding: 10px; border-bottom: 1px solid #ddd;">Please fill the form to provide Closure</td>
                    <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: center;">
                        <a href="{url}admin/web/formviaform/main/formfields/{form_id_next}?linked_mainaudit_id={linked_audit}&redirect=tasktome_secondary&is_detailbtn_form=1&session_module_id={module_id}" 
                        style="display: inline-block; padding: 8px 14px; background-color: #007bff; color: white; text-decoration: none; border-radius: 4px; font-weight: bold; font-size: 14px;">
                            Take Action
                        </a>
                    </td>
                </tr>
        '''

        sr_no = 1
        for ele in ans_image:
            sr_no +=1
            message += f'''
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #ddd;">{sr_no}</td>
                    <td style="padding: 10px; border-bottom: 1px solid #ddd;">Please find the image attachment for the referred details</td>
                    <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: center;">
                        <a href="{url}uploads/form_via_form/answer_images/{year}/{month}/{ele}" 
                        style="display: inline-block; padding: 8px 14px; background-color: #007bff; color: white; text-decoration: none; border-radius: 4px; font-weight: bold; font-size: 14px;">
                            View Image
                        </a>
                    </td>
                </tr>
            '''
        for ele in ans_attach:
            sr_no +=1
            message += f'''
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #ddd;">{sr_no}</td>
                    <td style="padding: 10px; border-bottom: 1px solid #ddd;">Please find the report attachment for the referred details</td>
                    <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: center;">
                        <a href="{url}uploads/form_via_form/answertype_attachment/{ele}" 
                        style="display: inline-block; padding: 8px 14px; background-color: #007bff; color: white; text-decoration: none; border-radius: 4px; font-weight: bold; font-size: 14px;">
                            View Report
                        </a>
                    </td>
                </tr>
            '''
        for ele in ans_attach_sec:
            sr_no +=1
            message += f'''
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #ddd;">{sr_no}</td>
                    <td style="padding: 10px; border-bottom: 1px solid #ddd;">Please find the report attachment for the referred details</td>
                    <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: center;">
                        <a href="{url}uploads/form_via_form/answer_images/{year}/{month}/{ele}" 
                        style="display: inline-block; padding: 8px 14px; background-color: #007bff; color: white; text-decoration: none; border-radius: 4px; font-weight: bold; font-size: 14px;">
                            View Report
                        </a>
                    </td>
                </tr>
            '''
        message += f'''
            </tbody>
        </table>
        <p style="margin-top: 20px; font-size: 14px; color: #555;">Best Regards,</p>
        <p style="font-size: 14px; color: #222;">Df-OS</p>

        <!-- Footer -->
        <div style="background-color: #f8f9fa; color: #555; padding: 10px; margin-top: 20px; display: flex; justify-content: space-between; align-items: center; font-size: 12px; border-top: 1px solid #ddd; border-radius: 0 0 6px 6px;">
            <div>© Powered by Df-OS</div>
            <div>
                <img src="{url}assets/image_used/logo_name.png" alt="dfos-logo" width="60">
            </div>
        </div>

    </div>
</body>
</html>
'''
        return message
    except Exception as e:
        print(f"Error in the function message_type: {e}")
        raise



# **kwargs allows you to pass a variable number of keyword arguments to a function. 
# These arguments are collected into a dictionary where the keys are the argument names and the values are the corresponding values passed.
# This is useful when you want to handle named arguments dynamically, without knowing them beforehand.
def main_code_submission_mail_for_multiple_forms(**kwargs):   


    # Extract values from the config
    audit_id = kwargs.get('audit_id')
    form_id = kwargs.get('form_id')
    url = kwargs.get('url')
    field_id = kwargs.get('field_id')
    attachment_field_id = kwargs.get('attachment_field_id')
    attachment_field_image_id = kwargs.get('attachment_field_image_id')
    form_name = kwargs.get('form_name')
    
    next_forms_dict = {}

    if 'conf' in kwargs and kwargs['conf']:
        try:
            conf = json.loads(kwargs['conf'])
            if isinstance(conf, list):
                for item in conf:
                    form_id = item.get('next_form_id')
                    users = item.get('form_users', '')
                    user_list = users.split(',') if users else []
                    next_forms_dict[form_id] = user_list
        except json.JSONDecodeError:
            pass  # Invalid JSON, ignore

    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Ensure all IDs are in list format to simplify downstream processing
    # Sometimes, configurator or API may send these as a single string instead of a list

    if not isinstance(field_id, list):
        field_id = [field_id]  # Convert to list if it's a single string
    if not isinstance(attachment_field_id, list):
        attachment_field_id = [attachment_field_id]
    if not isinstance(attachment_field_image_id, list):
        attachment_field_image_id = [attachment_field_image_id]

    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Print/log key variable values for debugging and auditing purposes
    # Helps trace issues if something fails during execution
    
    print(f"::group:: PARAMS =====")

    print("FORM ID:- ",form_id)
    print("AUDIT ID:- ", audit_id)
    print("NEXT FORM ID:- ", next_forms_dict)
    print("URL:- ", url)
    print("Attachment Field Id:- ", attachment_field_id)
    print("Attachment Image Field Id:- ", attachment_field_image_id)
    print("Form Name:- ", form_name)

    print("::endgroup::")

    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Setting up email subject and database connection
    # Note: Push notification logic is not implemented at this point

    connection = dfos_con.connection_for_all_replica(url)  
    # Read-only DB connection — uses the correct replica based on the given URL

    subject = f"{form_name} has been submitted"   # Email subject with dynamic form name

    bcc_email = "aviral.tanwar@designx.in"        # BCC email for QA/monitoring purposes

    bot_user_id = ""                              # Bot user ID (used if the bot needs to act on behalf of someone)

    connection = dfos_con.connection_for_all_replica(url)  
    # Read-only DB connection — uses the correct replica based on the given URL

    print("Email Subject:", subject)              # Log subject for debugging
    # Email will be sent from the 'donotreply@...' address — check spam filters or suppression rules if not received

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------MAIN CODE'S LOGIC STARTS FROM BELOW------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    print("-------------------------------------------------------------------------------------------------------------------")
    print("-------------------------------------------------------------------------------------------------------------------")


    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Fetch module ID and linked main audit ID from the audit table based on the current audit_id

    module_id, linked_main_audit_id = fetch_from_audits(connection, audit_id)
    print("Module ID:", module_id)

    # If there's no linked main audit (i.e., value is 0), default to the current audit ID
    if linked_main_audit_id == 0:
        linked_main_audit_id = audit_id

    print("Linked Main Audit ID:", linked_main_audit_id)

    # Fetching the user ID of the person who submitted the form
    submission_user_id = fetch_user_id(connection, audit_id)
    print("Submission User Id:- ", submission_user_id)


# Fetching CC email list of the user who submitted the form
    cc_email = users_func(connection, submission_user_id)
    print("cc_email :- ", cc_email)

    # Fetching name of the auditor (first and last name)
    first_name, last_name = fetch_name(connection, submission_user_id)
    print("first_name of the audtior:- ", first_name)
    print("last_name of the auditor:- ", last_name)

    # Fetching question and answer responses which needs to be sent in the mail
    question_name, answer, created_at = fetch_answer(connection, field_id, audit_id)
    print("Questions:- ", question_name)
    print("Answer:- ", answer)

    # DATE when the form was submitted 
    date_time = created_at.strftime("%d %B %Y %I:%M %p")
    print("date_time:- ", date_time)

    # Initialize attachment placeholders
    ans_attach = []
    ans_image = []
    ans_attach_sec = []
    year = 0
    month = 0

    # Fetch file-type attachments (PDF, docs, etc.) if available
    if attachment_field_id != [""]:
        field_type = "Attachment"

        ans_attach, ans_attach_sec = fetch_attachment(connection, attachment_field_id, audit_id, field_type)

        print("ans_attach:- ", ans_attach)
        print("ans_attach_sec:- ", ans_attach_sec)
    
    # Fetch image-type attachments (JPGs, PNGs, etc.) if available
    if attachment_field_image_id != [""]:

        field_type = "MultipleImage"

        year = created_at.year
        month = created_at.strftime("%m")  # Two-digit month (e.g., 04)

        print("Year:- ", year)
        print("Month:- ", month)

        ans_image = fetch_attachment_image(connection, attachment_field_image_id, audit_id, field_type)
        print("ans_image:- ", ans_image)

    for next_form_id, user_ids in next_forms_dict.items():
        print("Working for Form id:- ", next_form_id)
        print("User Id assigned to this form are :- ", user_ids)

        try:
            to_email = users_func(connection, user_ids)
            print(f"Sending email for Form ID {next_form_id} — Email: {to_email}")


            message = message_type(url, form_name, first_name, last_name, date_time,question_name, answer, ans_attach, ans_image,next_form_id, linked_main_audit_id, module_id, year, month, ans_attach_sec)

            bot_func.send_mail_with_to_cc_bcc_tag0(to_email, cc_email, bcc_email, subject, message)

        except Exception as e:
            print(f"Error processing user {user_ids} for form {next_form_id}: {e}")



    return "DesignX"
