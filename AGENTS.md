 # SUPER DAG

> **Purpose** &#x20;
> A data orchestration DAG designed to automate post-form submission workflows including sending notification emails, updating workflow statuses, reassigning users, scheduling follow-up forms, and triggering additional form submissions.

---

## DAG Name

`SUPER_DAG`

## DAG Trigger URL Format

```
https://airflow.dfos.co:8080/api/v1/dags/{company_name}_GlobalStandardBot_main_json_parser/dagRuns
```

Replace `{company_name}` with the appropriate server name. Codex should dynamically generate this URL based on client configuration.

---

## Trigger Configuration Format (JSON)

The DAG accepts a dynamic `dag_run.conf` JSON input that supports parallel task execution. Below are supported task keys and their respective input structures:

### 1. `submission_mail_for_single_form`

**Purpose:** Sends a mail after a form submission when the next form is a single form.

> **Note:** In this case, the recipients (`to`) will be users assigned to the next form, and the sender (`cc`) will be the user who submitted the current form.

```json
{
  "submission_mail_for_single_form": {
    "field_id": "",
    "attachment_field_id": "",
    "attachment_field_image_id": "",
    "form_name": ""
  }
}
```

**Field Descriptions:**

* `field_id`: Comma-separated question IDs whose answers should appear in the mail content.
* `attachment_field_id`: IDs of questions with uploaded documents to include in the email.
* `attachment_field_image_id`: IDs of questions with uploaded images to include in the email.
* `form_name`: Display name of the form in the email content.

> Multiple IDs can be passed as comma-separated values.

---

### 2. `submission_mail_for_multiple_forms`

**Purpose:** Sends a mail after a form submission when multiple follow-up forms are triggered.

```json
{
  "submission_mail_for_multiple_forms": {
    "field_id": "",
    "attachment_field_id": "",
    "attachment_field_image_id": "",
    "form_name": ""
  }
}
```

**Field Descriptions:**

* `field_id`: Comma-separated question IDs whose answers should appear in the mail content.
* `attachment_field_id`: IDs of questions with uploaded documents to include in the email.
* `attachment_field_image_id`: IDs of questions with uploaded images to include in the email.
* `form_name`: Display name of the form in the email content.

> Multiple IDs can be passed as comma-separated values.

### 3. `workflow_approval_status`

**Purpose:** Updates the workflow status of the current form and optionally notifies approvers or stakeholders.

```json
{
  "workflow_approval_status": {
    "workflow_status": ""
  }
}
```

**Field Descriptions:**

* `workflow_status`: Integer code representing the desired status.

**Supported Status Codes:**

* `0`: Pending
* `1`: Approved
* `2`: Rejected
* `3`: Draft
* `4`: Sent for Approval
* `5`: Pending at Approver 1
* `6`: Pending at Approver 2
* `7`: Master File
* `8`: Hidden

---

### 4. `nc_assign_based_on_master_form`

**Purpose:** In the main form, based on the value chosen in the question, all the NC's are assigned to the user which is in the master form (for the value chosen in the Main Form)

```json
{
    "nc_assign_based_on_master_form": {
	    "bot_user_id":"",
	    "universal_tag_of_master_form":"",
	    "universal_tag_of_field_id_for_user":""
    }
}
```
**Field Descriptions:**

* `bot_user_id`: A new or already existing bot user id which will be used by the code to have an authorization token
* `universal_tag_of_master_form`: Universal tag of the master form from which the user will be fetched based on the answer.
* `universal_tag_of_field_id_for_user`: Universal Tag ID present in the Question of the Master form and the Main Form from which we will compare the values
* `module_id_of_the_master_form`: Module id of the master form.

> 
---
### 5. `basic_schedule_form`

**Purpose:** This task is used to **schedule a form** based on responses submitted in a main form. It supports both **dynamic** and **static** configurations for form ID, module ID, users, dates, and more.

The scheduling action can either pull data from specific question IDs or use predefined static values.

```json
{
  "basic_schedule_form": {
    "module_field_id": "",
    "module_id": "",
    "form_field_id": "",
    "form_id_schedule": "",
    "schedule_field_users": "",
    "schedule_static_users": "",
    "bot_user_id": "",
    "start_date_field_id": "",
    "start_date": "",
    "end_date_field_id": "",
    "end_date": "",
    "approver_field_id": "",
    "card_details": ""
  }
}
```
**Field Descriptions:**

* `module_field_id`: 	(Optional) Question ID from which the module ID should be dynamically fetched. Only one value is allowed.
* `module_id`:  	(Optional) Static module ID of the form to be scheduled. Required if module_field_id is not used. Only one value is allowed.
* `form_field_id`: 	(Optional) Question ID from which the form ID should be dynamically fetched. Only one value is allowed.
* `form_id_schedule`: 	(Optional) Static form ID of the form to be scheduled. Required if form_field_id is not used. Only one value is allowed.
* `schedule_field_users`: (Optional) Question ID from which the user IDs should be dynamically fetched. Only one value is allowed.
* `schedule_static_users`:	(Optional) Comma-separated static user IDs to assign the scheduled form to.
* `bot_user_id`: 	(Required) ID of the bot user responsible for the scheduling action.
* `start_date_field_id`: 	(Optional) Static start date for the scheduled form. Required if start_date_field_id is not used. 
* `start_date`: If it's static, the it must be passed here i.e. the start date of the form which needs to be scheduled. 
* `end_date_field_id`: (Optional) Question ID from which the end date should be dynamically fetched.
* `end_date`:  (Optional) Static end date for the scheduled form. Required if end_date_field_id is not used. It is in number and logic is ( no of days+ Start Date)
* `approver_field_id`: If there is a secondary form, then the question id where its being approved or rejected needs to be here
* `card_details`: All the question ids which needs to be shown on the card in the schedule option

> The workflow for this BOT involves a **main form** that contains questions to configure the scheduling of **secondary forms**.  
> Based on the submitted values, the BOT either **approves or rejects** and schedules follow-up forms accordingly.
> If `module_field_id` is provided, `module_id` **must** be an empty string (`""`), and **vice versa**.
> If `form_field_id` is provided, `form_id_schedule` **must** be an empty string, and **vice versa**.
> If `start_date_field_id` is provided, `start_date` **must** be an empty string, and **vice versa**.
> If `end_date_field_id` is provided, `end_date` **must** be an empty string, and **vice versa**.
 
---
---

## Examples

Q) I want a bot for hmcl in which the dag sends a mail to the next single form
for the field ids 123,345,3211 and name of the form is Gemba and there is an image in the question id 55643. Also the bot will change the workflow of the form to Rejected when the form is submitted

Ans) BOT URL -> https://airflow.dfos.co:8080/api/v1/dags/hmcl_GlobalStandardBot_main_json_parser/dagRuns
BOT CONFIG -> 
```JSON
{
  "submission_mail_for_single_form": {
    "field_id": "",
    "attachment_field_id": "",
    "attachment_field_image_id": "",
    "form_name": ""
  },
  "workflow_approval_status": {
    "workflow_status": ""
  }
}
```
---
<<<<<<< HEAD

Q) In amber, I want to send a mail to the next multiple forms users and the mail content should have the foprm name as GEMBA.
The form is used for scheduling other forms too. The form which it schedules are in module id 50.
It chooses the form id from question id 1.
meanwhile the user id is 70. and bot user id is too 70.
The form's scheduling date will be chosen from 2 question id and the form can only be filled within next 2 weeks.The schedule option should show 1,2,3 on the main screen (in the card).
And the workflow needs to be Hidden

Ans) BOT URL -> https://airflow.dfos.co:8080/api/v1/dags/amber_GlobalStandardBot_main_json_parser/dagRuns

BOT CONFIG -> 
```JSON
{
  "submission_mail_for_multiple_forms": {
    "field_id": "",
    "attachment_field_id": "",
    "attachment_field_image_id": "",
    "form_name": "GEMBA"
  },
  "basic_schedule_form": {
    "module_field_id": "",
    "module_id": "50",
    "form_field_id": "1",
    "form_id_schedule": "",
    "schedule_field_users": "",
    "schedule_static_users": "70",
    "bot_user_id": "70",
    "start_date_field_id": "2",
    "start_date": "",
    "end_date_field_id": "",
    "end_date": "14",
    "approver_field_id": "",
    "card_details": "1,2,3"
  },
  "workflow_approval_status": {
    "workflow_status": "8"
  }
}

```
---
=======
>>>>>>> ad88ad9924612312293965fdc6866149730e8ff4
