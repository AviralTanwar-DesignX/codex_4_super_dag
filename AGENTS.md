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

---

## Examples

Q) I want a bot for hmcl in which the dag sends a mail to the next single form
for the field ids 123,345,3211 and name of the form is Gemba and there is an image in the question id 55643. Also the bot will change the workflow of the form to Rejected when the form is submitted

Ans) https://airflow.dfos.co:8080/api/v1/dags/hmcl_GlobalStandardBot_main_json_parser/dagRuns

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
