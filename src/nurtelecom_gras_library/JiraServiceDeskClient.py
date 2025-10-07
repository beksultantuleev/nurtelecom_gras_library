from typing import Optional, Union, Dict, List
import requests
import os
from requests.auth import HTTPBasicAuth
import pandas as pd

class JiraClient:
    """
    A client for interacting with both Jira Service Desk and Jira Software (Project) APIs.
    """
    def __init__(
        self,
        base_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None
    ):
        """
        Initializes the Jira client.

        Args:
            base_url (str): The base URL of your Jira instance (e.g., "https://your-domain.atlassian.net").
            username (Optional[str]): Your Jira username for basic authentication.
            password (Optional[str]): Your Jira password or API token for basic authentication.
            token (Optional[str]): A Personal Access Token (PAT) for Bearer token authentication.
        """
        self.base_url = base_url.rstrip('/')

        if token:
            self.auth = None
            self.headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
        elif username and password:
            self.auth = HTTPBasicAuth(username, password)
            self.headers = {
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
        else:
            raise ValueError("Either 'token' or both 'username' and 'password' must be provided.")
    
        # =================================================================
        # V.2 CORE API METHODS (FOR STANDARD JIRA PROJECTS)
    # =================================================================

    def create_project_issue(self, project_key: str, issue_type_name: str, fields: dict) -> Optional[dict]:
        """
        Creates an issue in a standard Jira project.

        Args:
            project_key (str): The key of the project (e.g., "OPTM").
            issue_type_name (str): The name of the issue type (e.g., "Task", "Bug").
            fields (dict): A dictionary of fields for the issue. Must include 'summary'.
                           Example: {"summary": "My new task", "description": "Details...", "customfield_12345": "Value"}

        Returns:
            Optional[dict]: The response from Jira containing issue details (key, id, self) or None on failure.
        """
        url = f"{self.base_url}/rest/api/2/issue"
        
        try:
            meta = self._get_project_meta(project_key, issue_type_name)
        except ValueError as e:
            print(f"❌ Error getting project metadata: {e}")
            return None

        payload = {
            "fields": {
                "project": {"id": meta["project_id"]},
                "issuetype": {"id": meta["issue_type_id"]},
            }
        }
        
        # Populate fields, handling special cases like users or select options if necessary
        for key, value in fields.items():
            if key in meta["fields_meta"]:
                schema = meta["fields_meta"][key].get("schema", {})
                field_type = schema.get("type")
                
                if field_type == "option":
                    payload["fields"][key] = {"id": str(value)} # Ensure value is a string if it's an ID
                elif field_type == "user":
                    payload["fields"][key] = {"name": value}
                else:
                    payload["fields"][key] = value
            else:
                 payload["fields"][key] = value # Add field even if not in meta (like summary, description)

        resp = requests.post(url, json=payload, headers=self.headers, auth=self.auth)
        
        if resp.status_code == 201:
            # return resp.json()
            return resp.json()['key']
        else:
            print(f"❌ Error creating project issue: {resp.status_code}, {resp.text}")
            return None

    def add_attachment_to_issue(self, issue_key: str, file_path: str) -> bool:
        """
        Attaches a file to any existing Jira issue (Project or Service Desk).

        Args:
            issue_key (str): The issue key (e.g., "OPTM-123" or "ITSD-456").
            file_path (str): The local path to the file to attach.

        Returns:
            bool: True if attachment was successful, False otherwise.
        """
        url = f"{self.base_url}/rest/api/2/issue/{issue_key}/attachments"
        
        headers = self.headers.copy()
        headers.pop("Content-Type", None)  # Let requests handle the multipart content type
        headers["X-Atlassian-Token"] = "no-check"

        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            return False

        with open(file_path, "rb") as f:
            files = {"file": (os.path.basename(file_path), f)}
            resp = requests.post(url, files=files, headers=headers, auth=self.auth)

        if resp.status_code == 200:
            print(f"✅ Successfully attached {os.path.basename(file_path)} to {issue_key}")
            return True
        else:
            print(f"❌ Error attaching file to {issue_key}: {resp.status_code}, {resp.text}")
            return False

    def _get_project_meta(self, project_key: str, issue_type_name: str) -> dict:
        """Helper to get metadata required for creating an issue."""
        url = f"{self.base_url}/rest/api/2/issue/createmeta"
        params = {"projectKeys": project_key, "expand": "projects.issuetypes.fields"}
        
        resp = requests.get(url, auth=self.auth, params=params, headers=self.headers)
        resp.raise_for_status()
        data = resp.json()

        project = next((p for p in data["projects"] if p["key"] == project_key), None)
        if not project:
            raise ValueError(f"Project '{project_key}' not found or you don't have permission.")

        issue_type = next((it for it in project["issuetypes"] if it["name"] == issue_type_name), None)
        if not issue_type:
            raise ValueError(f"Issue type '{issue_type_name}' not found in project '{project_key}'.")

        return {
            "project_id": project["id"],
            "issue_type_id": issue_type["id"],
            "fields_meta": issue_type["fields"]
        }

    def get_project_issue_fields(self, project_key: str, issue_type_name: str) -> Optional[dict]:
        """
        Returns a description of all fields for a given project and issue type.
        """
        try:
            meta = self._get_project_meta(project_key, issue_type_name)
            fields_info = {}
            for field_id, info in meta["fields_meta"].items():
                 fields_info[field_id] = {
                    "name": info.get("name"),
                    "required": info.get("required", False),
                    "type": info.get("schema", {}).get("type", "unknown"),
                    "allowedValues": info.get("allowedValues")
                }
            return fields_info
        except (ValueError, requests.HTTPError) as e:
            print(f"❌ Could not get fields: {e}")
            return None

    # Add this method inside your JiraClient class
    def add_comment_to_issue(self, issue_key: str, comment_body: str) -> bool:
        """
        Adds a comment to any existing Jira issue (Project or Service Desk).

        Args:
            issue_key (str): The issue key (e.g., "OPTM-123").
            comment_body (str): The text of the comment to add.

        Returns:
            bool: True if the comment was added successfully, False otherwise.
        """
        url = f"{self.base_url}/rest/api/2/issue/{issue_key}/comment"
        payload = {"body": comment_body}
        
        resp = requests.post(url, json=payload, headers=self.headers, auth=self.auth)
        
        if resp.status_code == 201:
            print(f"💬 Successfully added comment to {issue_key}")
            return True
        else:
            print(f"❌ Error adding comment to {issue_key}: {resp.status_code}, {resp.text}")
            return False
        
    # Inside your JiraClient class
    def add_comment_with_attachments(self, issue_key: str, comment_body: str, file_paths: Union[str, List[str]]) -> bool:
        """
        Adds a comment and embeds one or more attachments within it.

        This is a multi-step process:
        1. Uploads each file as a standard attachment.
        2. Posts a comment with the given text and Jira markup (!filename.ext|thumbnail!) 
           to display the attachments.

        Args:
            issue_key (str): The issue key (e.g., "OPTM-123").
            comment_body (str): The text of the comment.
            file_paths (Union[str, List[str]]): A single file path or a list of file paths.

        Returns:
            bool: True if all files were attached and the comment was posted successfully.
        """
        # Ensure file_paths is a list for consistent processing
        if isinstance(file_paths, str):
            file_paths = [file_paths]

        uploaded_filenames = []
        # Step 1: Upload all files as attachments
        for file_path in file_paths:
            if not self.add_attachment_to_issue(issue_key, file_path):
                print(f"❌ Halting process because attachment failed for: {file_path}")
                return False # Stop if any attachment fails
            uploaded_filenames.append(os.path.basename(file_path))

        # Step 2: Construct the final comment with Jira markup for each attachment
        full_comment = comment_body
        if uploaded_filenames:
            full_comment += "\n\n" # Add some spacing
            for filename in uploaded_filenames:
                # Jira markup for embedding an image or file link
                full_comment += f"!{filename}|thumbnail!\n"
        
        # Step 3: Post the comment
        return self.add_comment_to_issue(issue_key, full_comment)

    # =================================================================
    # SERVICE DESK API METHODS (Your original methods, renamed for clarity)
    # =================================================================

    def create_service_desk_request(self, service_desk_id: str, request_type_id: str, fields: dict) -> Optional[str]:
        """
        Creates a request in a Jira Service Desk portal.
        """
        url = f"{self.base_url}/rest/servicedeskapi/request"
        payload = {
            "serviceDeskId": service_desk_id,
            "requestTypeId": request_type_id,
            "requestFieldValues": fields
        }
        resp = requests.post(url, json=payload, headers=self.headers, auth=self.auth)
        if resp.status_code in (200, 201):
            return resp.json().get("issueKey")
        print(f"❌ Ошибка создания заявки: {resp.status_code}, {resp.text}")
        return None

    def add_comment_to_request(self, issue_key: str, text: str, public: bool = True):
        """
        Adds a comment to a Service Desk request.
        """
        url = f"{self.base_url}/rest/servicedeskapi/request/{issue_key}/comment"
        payload = {"body": text, "public": public}
        resp = requests.post(url, json=payload, headers=self.headers, auth=self.auth)
        if resp.status_code != 201:
            print(f"❌ Ошибка добавления комментария: {resp.status_code}, {resp.text}")


    def attach_to_request(self, issue_key: str, tmp_attachment_id: str, comment: str):

        url = f"{self.base_url}/rest/servicedeskapi/request/{issue_key}/attachment"
        headers = self.headers.copy()
        headers.update({
            "X-ExperimentalApi": "opt-in",
            "X-Atlassian-Token": "no-check"
        })
        payload = {
            "temporaryAttachmentIds": [tmp_attachment_id],
            "public": True,
            "additionalComment": {"body": comment}
        }

        resp = requests.post(
            url, json=payload, headers=headers, auth=self.auth)
        if resp.status_code not in (200, 201, 204):
            print(
                f"❌ Ошибка прикрепления файла: {resp.status_code}, {resp.text}")
    
    def get_request_details(self, issue_key: str) -> Optional[dict]:
        url_main = f"{self.base_url}/rest/servicedeskapi/request/{issue_key}"
        url_issue = f"{self.base_url}/rest/api/2/issue/{issue_key}"

        r1 = requests.get(url_main, headers=self.headers, auth=self.auth)
        r2 = requests.get(url_issue, headers=self.headers, auth=self.auth)

        if r1.status_code != 200:
            print(
                f"❌ Ошибка получения основной информации: {r1.status_code}, {r1.text}")
            return None

        main_data = r1.json()
        issue_data = r2.json() if r2.status_code == 200 else {}

        return {
            "createdDate": main_data.get("createdDate", {}).get("iso8601"),
            "currentStatus": main_data.get("currentStatus", {}).get("status"),
            "reporter": main_data.get("reporter", {}).get("name"),
            "resolutionDate": issue_data.get("fields", {}).get("resolutiondate"),
            "jira_key": issue_key
        }

    def check_portal_access(self):
        url = f"{self.base_url}/rest/servicedeskapi/servicedesk"
        try:
            response = requests.get(url, headers=self.headers, auth=self.auth)
            if response.status_code == 200:
                print("✅ Доступные порталы и проекты:")
                data = response.json()
                for item in data.get("values", []):
                    print(f"- ID: {item['id']} | Project Name: {item['projectName']}")
            else:
                print(f"❌ Ошибка получения данных: {response.status_code}")
        except requests.RequestException as e:
            print(f"❌ Ошибка запроса: {e}")

    def get_request_types(self, service_desk_id: Union[str, int]):
        url = f"{self.base_url}/rest/servicedeskapi/servicedesk/{service_desk_id}/requesttype"

        resp = requests.get(url, headers=self.headers, auth=self.auth)

        if resp.status_code == 200:
            print(f"✅ Типы заявок для портала {service_desk_id}:")
            for item in resp.json().get("values", []):
                print(f"- ID: {item['id']} | Name: {item['name']}")
        else:
            print(
                f"❌ Ошибка при получении типов заявок: {resp.status_code} — {resp.text}")

    def get_request_fields(self, service_desk_id: Union[str, int], request_type_id: Union[str, int]):
        '''
        gives field information
        '''
        url = f"{self.base_url}/rest/servicedeskapi/servicedesk/{service_desk_id}/requesttype/{request_type_id}/field"

        resp = requests.get(url, headers=self.headers, auth=self.auth)
        if resp.status_code == 200:
            print(
                f"✅ Поля формы для Request Type {request_type_id} (Portal {service_desk_id}):")
            for f in resp.json().get("requestTypeFields", []):
                print(
                    f"- {f['fieldId']} | {f['name']} | required={f.get('required')}")
        else:
            print(
                f"❌ Ошибка при получении полей формы: {resp.status_code} — {resp.text}")

    def add_user_to_task(self, login: str, issue_key: str):
        """
        Adds a user as a participant to the request so they can view it.

        Args:
            login (str): Jira username of the user to add as a participant.
            issue_key (str): The issue key (e.g., "ITSD-123").

        Raises:
            Exception: Raises an exception if the request fails (status code not in 200, 201, or 204).

        Note:
            This method uses the Jira Service Desk API endpoint:
            POST /rest/servicedeskapi/request/{issueKey}/participant
        """
        url = f"{self.base_url}/rest/servicedeskapi/request/{issue_key}/participant"
        headers = self.headers.copy()
        headers.update({
            "X-ExperimentalApi": "opt-in",
            "X-Atlassian-Token": "no-check"
        })
        payload = {
             "usernames": login
        }

        resp = requests.post(url, json=payload, headers=headers, auth=self.auth)
        if resp.status_code not in (200, 201, 204):
            raise Exception(
                f"❌ Ошибка добавления пользователя {login} в заявку {issue_key}: {resp.status_code}, {resp.text}")
    
    # =================================================================
    # GENERIC HELPER METHODS (Work for both)
    # =================================================================
    
    def fetch_all_issues(self, jql: str, batch_size=50) -> List[dict]:
        # This method is already using the core API, so it's perfect as-is.
        all_issues = []
        start_at = 0
        while True:
            params = {"jql": jql, "startAt": start_at, "maxResults": batch_size}
            response = requests.get(
                f"{self.base_url}/rest/api/2/search", headers=self.headers, params=params, auth=self.auth
            )
            if response.status_code != 200:
                print(f"❌ Failed to fetch issues at startAt={start_at}: {response.status_code}")
                print(response.text)
                break
            data = response.json()
            issues = data.get("issues", [])
            all_issues.extend(issues)
            if start_at + len(issues) >= data.get("total", 0):
                break
            print(f'retrieved {start_at + len(issues)} of {data.get("total", 0)}')
            start_at += batch_size
        return all_issues

    # ... [Keep get_fields_summary and get_issues_df as they are generic] ...

    def get_fields_summary(self, issues: list[dict]) -> dict:
        """
        Extracts a dictionary where each key is a top-level field in 'fields',
        and the value is a set of all sub-keys found for that field across all issues.
        For fields that are not dicts, the value will be an empty set.

        Returns:
            dict: {field_name: set(sub_keys)}
        """
        summary = {}
        for issue in issues:
            fields = issue.get("fields", {})
            for key, value in fields.items():
                if key not in summary:
                    summary[key] = set()
                if isinstance(value, dict):
                    summary[key].update(value.keys())
        return summary

    def get_issues_df(self, issues, field_map):
        """
        Processes a list of Jira issues and returns a DataFrame with columns as specified in field_map.
        field_map is a dict where:
            - key: field in Jira (e.g., 'summary', 'created', 'status')
            - value: 
                - None: take the field as is (from issue['fields'][key])
                - str: subkey to extract from a dict field (e.g., 'status':'name' extracts issue['fields']['status']['name'])
                - dict: subkeys to extract from a dict field (e.g., 'status': {'self': None, 'statusCategory': None})
                - 'Key': special value to extract the issue key

        Args:
            issues (list): List of Jira issue dicts.
            field_map (dict): Mapping of Jira field names to DataFrame column names or subkeys.

        Returns:
            pd.DataFrame: DataFrame containing the selected fields for each issue.
        """
        data = []
        for issue in issues:
            row = {}
            for field, subkey in field_map.items():
                if field == 'Key':
                    row['Key'] = issue.get('key')
                elif subkey is None:
                    row[field] = issue.get('fields', {}).get(field)
                elif isinstance(subkey, str):
                    value = issue.get('fields', {}).get(field)
                    if isinstance(value, dict):
                        row[subkey] = value.get(subkey)
                    else:
                        row[subkey] = None
                elif isinstance(subkey, dict):
                    value = issue.get('fields', {}).get(field, {})
                    for subfield in subkey:
                        row[subfield] = value.get(subfield)
            data.append(row)
        if data:
            df = pd.DataFrame(data)
            return df

    def list_available_projects(self) -> Optional[List[dict]]:
        """
        Lists all standard Jira projects accessible by the current user.
        This is the equivalent of 'check_portal_access' for non-Service Desk projects.

        Returns:
            Optional[List[dict]]: A list of project dictionaries on success, None on failure.
        """
        url = f"{self.base_url}/rest/api/2/project"
        print("🔎 Checking for available standard Jira projects...")
        try:
            response = requests.get(url, headers=self.headers, auth=self.auth)
            if response.status_code == 200:
                projects = response.json()
                print("✅ Available Projects:")
                for project in projects:
                    print(f"- Key: {project['key']} | Name: {project['name']} | ID: {project['id']}")
                return projects
            else:
                print(f"❌ Error getting project data: {response.status_code} - {response.text}")
                return None
        except requests.RequestException as e:
            print(f"❌ Request error: {e}")
            return None

    def list_issue_types_for_project(self, project_key: str) -> Optional[List[dict]]:
        """
        Lists all available issue types for a specific Jira project.

        Args:
            project_key (str): The key of the project (e.g., "OPTM").

        Returns:
            Optional[List[dict]]: A list of issue type dictionaries on success, None on failure.
                                  Each dict contains 'id', 'name', 'description'.
        """
        url = f"{self.base_url}/rest/api/2/issue/createmeta"
        params = {"projectKeys": project_key, "expand": "projects.issuetypes"}
        print(f"🔎 Fetching issue types for project '{project_key}'...")
        
        try:
            resp = requests.get(url, auth=self.auth, params=params, headers=self.headers)
            resp.raise_for_status() # Raises an exception for bad status codes
            data = resp.json()

            project = next((p for p in data.get("projects", []) if p["key"] == project_key), None)
            if not project:
                print(f"❌ Project '{project_key}' not found or you don't have permission.")
                return None

            issue_types = project.get("issuetypes", [])
            if not issue_types:
                print(f"🤷 No issue types found for project '{project_key}'.")
                return []

            print(f"✅ Available Issue Types for '{project_key}':")
            result = []
            for itype in issue_types:
                print(f"- Name: \"{itype['name']}\" (ID: {itype['id']})")
                result.append({
                    "id": itype.get('id'),
                    "name": itype.get('name'),
                    "description": itype.get('description', '')
                })
            return result

        except requests.HTTPError:
            print(f"❌ Error fetching data: {resp.status_code} - {resp.text}")
            return None
        except requests.RequestException as e:
            print(f"❌ Request error: {e}")
            return None


if __name__ == "__main__":

    JIRA_TOKEN = 'JIRA_TOKEN'#os.environ.get('JIRA_TOKEN')
    JIRA_username = 'JIRA_username'#os.environ.get('JIRA_username')
    JIRA_password = 'JIRA_password'#os.environ.get('JIRA_password')
    jira_client = JiraClient(
        base_url='https://xxx', token=JIRA_TOKEN)
    # jira_client = JiraServiceDeskClient(
    #     base_url='https://sd.o.kg', username='complaints_bot', password='complaints_bot')
    # jira_client.check_portal_access()
    # jira_client.get_request_types(service_desk_id=80)
    # jira_client.get_request_fields(request_type_id=1054, service_desk_id=80)

    test_fields = {
        "summary": "🔧 Тестовая заявка из скрипта",
        "description": (
            "🧪 Это тестовая заявка, созданная из Python-скрипта.\n"
            "📆 Дата: 2025-07-03\n"
            "💬 Проверка API интеграции."
        )
    }

    # jira_client.list_available_projects()


    # issue_key = jira_client.create_request(
    #     fields=test_fields, service_desk_id=80, request_type_id=1054)
    # print(issue_key)
    issue_key = 'NT-420316'
    attachment_id = jira_client.upload_attachment_to_issue(
        service_desk_id=80, file_path='./bot_image/complaint_pic.jpg')
    jira_client.attach_to_request(
        issue_key=issue_key, comment='test photo1', tmp_attachment_id=attachment_id)
