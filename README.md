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

**Purpose:** In the main form

```json
{
    "nc_assign_based_on_master_form": {
	    "bot_user_id":"",
	    "universal_tag_of_master_form":"",
	    "universal_tag_of_field_id_for_user":""
    }
}
```

---

### 5. `form_submission_on_planning`

**Purpose:** Automatically submits a planning form as part of a pre-audit or strategic phase.

```json
{
  "form_submission_on_planning": {
    "form_id": "67890",
    "linked_audit_id": "345",
    "prepopulate_fields": {
      "site_name": "New Plant",
      "shift": "A"
    }
  }
}
```

---

> Multiple task keys can be included in a single DAG trigger JSON to support concurrent execution. Codex is responsible for assembling the JSON based on client requirements.

---

## Examples

