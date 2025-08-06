from typing import Optional, Union
import requests
import os
from requests.auth import HTTPBasicAuth

class JiraServiceDeskClient:
    def __init__(
        self,
        base_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None
    ):
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.username = username
        self.password = password

        if token:
            self.headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
            self.auth = None
        else:
            self.headers = {
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
            self.auth = HTTPBasicAuth(self.username, self.password)
    
    def create_request(self, service_desk_id: str, request_type_id: str, fields: dict) -> Optional[str]:
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

    def upload_attachment_to_issue(self, service_desk_id: str, file_path: str) -> Optional[str]:
        url = f"{self.base_url}/rest/servicedeskapi/servicedesk/{service_desk_id}/attachTemporaryFile"

        headers = self.headers.copy()
        headers.update({
            "X-ExperimentalApi": "opt-in",
            "X-Atlassian-Token": "no-check"
        })

        # Удаляем Content-Type, чтобы requests сам его добавил для multipart
        headers.pop("Content-Type", None)

        if not os.path.exists(file_path):
            print(f"❌ Файл не найден: {file_path}")
            return None

        with open(file_path, "rb") as f:
            files = {"file": f}
            try:
                resp = requests.post(url, files=files, headers=headers, auth=self.auth)
            except Exception as e:
                print(f"❌ Ошибка при отправке запроса: {e}")
                return None

        if resp.status_code == 201:
            try:
                data = resp.json()
                return data['temporaryAttachments'][0]['temporaryAttachmentId']
            except (ValueError, KeyError, IndexError) as e:
                print(f"❌ Ошибка разбора ответа: {e}, текст: {resp.text}")
                return None
        else:
            print(f"❌ Ошибка загрузки файла: {resp.status_code}, {resp.text}")
            return None

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

    def add_comment(self, issue_key: str, text: str):
        url = f"{self.base_url}/rest/servicedeskapi/request/{issue_key}/comment"
        payload = {"body": text, "public": True}
        resp = requests.post(
            url, json=payload, headers=self.headers, auth=self.auth)
        if resp.status_code != 201:
            print(
                f"❌ Ошибка добавления комментария: {resp.status_code}, {resp.text}")

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
        headers = {"Accept": "application/json"}

        try:
            response = requests.get(url, headers=headers, auth=self.auth)
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


if __name__ == "__main__":

    JIRA_TOKEN = 'JIRA_TOKEN'#os.environ.get('JIRA_TOKEN')
    JIRA_username = 'JIRA_username'#os.environ.get('JIRA_username')
    JIRA_password = 'JIRA_password'#os.environ.get('JIRA_password')
    jira_client = JiraServiceDeskClient(
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


    # issue_key = jira_client.create_request(
    #     fields=test_fields, service_desk_id=80, request_type_id=1054)
    # print(issue_key)
    issue_key = 'NT-420316'
    attachment_id = jira_client.upload_attachment_to_issue(
        service_desk_id=80, file_path='./bot_image/complaint_pic.jpg')
    jira_client.attach_to_request(
        issue_key=issue_key, comment='test photo1', tmp_attachment_id=attachment_id)
