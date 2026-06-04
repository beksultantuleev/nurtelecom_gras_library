import pandas as pd
import re
import os
import logging
from datetime import datetime
from typing import Optional, Tuple

try:
    import tableauserverclient as TSC
except ImportError:  # optional dependency; see the [tableau] extra
    TSC = None

logger = logging.getLogger(__name__)


class TableauServerManager:
    def __init__(self, login, password,
                 host: str,
                 server_version=None,
                 proxies: Optional[dict] = None) -> None:
        """
        Manage a Tableau Server connection and export views.

        :param login: Tableau username.
        :param password: Tableau password.
        :param host: Server URL, e.g. "https://your-tableau-host".
        :param server_version: Pin a specific REST API version (e.g. "3.9").
                               If None (default), the version is auto-detected
                               from the server, which avoids reflection crashes
                               on newer Tableau builds.
        :param proxies: Optional requests-style proxies dict.
        """
        if TSC is None:
            raise ImportError(
                "TableauServerManager requires the 'tableauserverclient' package. "
                "Install it with: pip install nurtelecom_gras_library[tableau]"
            )
        self.login = login
        self.password = password
        self.host = host.lower()
        self.host_https = self.host.replace('http://', 'https://')
        self.use_ssl = True
        self.proxies = proxies or {}
        self.server_version = server_version

        # Auto-detect server version unless one is explicitly pinned. This
        # prevents Spring Boot reflection crashes on newer Tableau servers.
        self.tableau_auth = TSC.TableauAuth(self.login, self.password)
        self.server = TSC.Server(
            self.host,
            use_server_version=(server_version is None),
            http_options={"verify": self.use_ssl, 'proxies': self.proxies},
        )
        if server_version is not None:
            self.server.version = server_version

        self.all_views_raw = []
        self.processed_df = None
        self.accepted_formats_to_export = ['png', 'pdf', 'csv', 'xlsx']

        self.exported_files_folder = 'exported_files'
        self.create_folder(self.exported_files_folder)

    def create_folder(self, folder_path):
        """Create a directory (and parents) if it does not already exist.

        :param folder_path: Directory path to create.
        :raises OSError: If the directory cannot be created.
        """
        try:
            os.makedirs(folder_path, exist_ok=True)
        except OSError as e:
            logger.error(f"Creation of the directory {folder_path} failed due to {e}")
            raise

    def update_all_views(self):
        'get all views as is (paginated)'
        self.all_views_raw = []
        with self.server.auth.sign_in(self.tableau_auth):
            req_option = TSC.RequestOptions()
            req_option.page_size = 100

            current_page = 1
            while True:
                req_option.pagenumber = current_page
                try:
                    views, pagination_item = self.server.views.get(req_option)
                    if not views:
                        break
                    self.all_views_raw.extend(views)

                    if current_page * req_option.page_size >= pagination_item.total_available:
                        break
                    current_page += 1
                except TSC.ServerResponseError as e:
                    logger.error(f"Failed to fetch page {current_page}: {e}")
                    break

        self.server.auth.sign_out()

    def make_standart_url(self, old_url):
        'uses only https, not http, and maps internal TSC URL to browser URL'
        pattern_for_sheets = "/sheets"
        modified_url = re.sub(pattern_for_sheets, "", old_url)
        new_url = f"{self.host_https}/#/views/{modified_url}"
        return new_url

    def compile_processed_df(self):
        'create pandas df with fixed url (id, name, url)'
        data = []
        for view in self.all_views_raw:
            data.append({
                'ID':   view.id,
                'Name': view.name,
                'URL':  self.make_standart_url(view.content_url),
            })
        self.processed_df = pd.DataFrame(data)

    def clean_tableau_url(self, url: str) -> Tuple[str, Optional[str]]:
        """
        Normalize a Tableau view URL by:
        - ensuring the HTTPS scheme,
        - removing the ':iid' query parameter,
        - extracting a full 'REG_ORD=...' filter expression (if present).

        :return: (clean_url, filter_expression_or_None)
        """
        if 'https://' not in url:
            url = url.replace('http://', 'https://')

        reg_ord_expr = None
        reg_match = re.search(r'[?&](REG_ORD=[^&?#]+)', url)
        if reg_match:
            reg_ord_expr = reg_match.group(1)

        url = re.sub(r'[?&]:iid=\d+', '', url)
        url = re.sub(r'[?&]REG_ORD=[^&?#]+', '', url)
        url = re.sub(r'[?&]+$', '', url)
        return url, reg_ord_expr

    def export_view(self, url, format='png'):
        '''
        Export a single Tableau view by its browser URL.

        Instead of enumerating every view on the site (which can crash on some
        servers), this resolves the workbook directly from the URL slug and
        populates only that workbook's views.

        accepted formats: png, pdf, csv, xlsx
        :return: (status_message, file_path_or_None)
        '''
        format = format.lower()
        if format not in self.accepted_formats_to_export:
            return f'No such format as {format}!', None

        # Clean the URL and extract any filters (e.g., REG_ORD).
        clean_url, filter_expression = self.clean_tableau_url(url)

        # Extract workbook and view slugs directly from the clean URL, e.g.
        # 'https://<host>/#/views/<workbook_slug>/<view_slug>'
        parts = clean_url.split('#/views/')
        if len(parts) < 2:
            return "Invalid URL format. Missing '#/views/'.", None

        path_parts = parts[1].strip('/').split('/')
        if len(path_parts) < 2:
            return "URL does not contain both workbook and view paths.", None

        workbook_slug = path_parts[0]
        view_slug = path_parts[1]

        with self.server.auth.sign_in(self.tableau_auth):
            # 1. Fetch ONLY the target workbook to bypass the site-wide crash bug.
            req_option = TSC.RequestOptions()
            req_option.filter.add(TSC.Filter(
                TSC.RequestOptions.Field.ContentUrl,
                TSC.RequestOptions.Operator.Equals,
                workbook_slug,
            ))

            try:
                workbooks, _ = self.server.workbooks.get(req_option)
            except Exception as e:
                return f"Server failed to fetch workbook '{workbook_slug}': {e}", None

            if not workbooks:
                return f"Workbook '{workbook_slug}' not found on server.", None

            target_workbook = workbooks[0]

            # 2. Populate the views ONLY for this specific, healthy workbook.
            try:
                self.server.workbooks.populate_views(target_workbook)
            except Exception as e:
                return f"Failed to get views for workbook '{workbook_slug}': {e}", None

            # 3. Find our specific view inside this safe list.
            # Tableau's internal content_url format is "workbook_slug/sheets/view_slug".
            expected_content_url = f"{workbook_slug}/sheets/{view_slug}"
            view_to_export = next(
                (v for v in target_workbook.views if v.content_url == expected_content_url),
                None,
            )

            if not view_to_export:
                return f"View '{view_slug}' not found inside workbook '{workbook_slug}'.", None

            # Image filter options (applied to PNG export).
            img_req = TSC.ImageRequestOptions(maxage=1)
            if filter_expression:
                k, v = filter_expression.split('=', 1)
                img_req.vf(k, v)

            try:
                file_path = (
                    f'{self.exported_files_folder}/{view_to_export.name}'
                    f'_{datetime.now().strftime("%Y%m%d_%H%M%S")}.{format}'
                )

                if format == 'png':
                    self.server.views.populate_image(view_to_export, img_req)
                    with open(file_path, 'wb') as file:
                        file.write(view_to_export.image)

                elif format == 'pdf':
                    self.server.views.populate_pdf(view_to_export)
                    with open(file_path, 'wb') as file:
                        file.write(view_to_export.pdf)

                elif format == 'csv':
                    self.server.views.populate_csv(view_to_export)
                    if hasattr(view_to_export, 'csv'):
                        with open(file_path, 'wb') as file:
                            for data in view_to_export.csv:
                                file.write(data)

                elif format == 'xlsx':
                    self.server.views.populate_excel(view_to_export)
                    with open(file_path, 'wb') as file:
                        for data in view_to_export.excel:
                            file.write(data)

                return (
                    f"SUCCESS! {clean_url} exported as "
                    f"{view_to_export.name}.{format}",
                    file_path,
                )

            except Exception as e:
                return f'Error during file generation: {e}', None


if __name__ == "__main__":
    # Generic example — supply your own credentials/host via environment.
    login = os.environ.get('TABLEAU_LOGIN')
    passwd = os.environ.get('TABLEAU_PASSWORD')
    host = os.environ.get('TABLEAU_HOST', 'https://your-tableau-host')

    example_url = f'{host}/#/views/your_workbook/your_view?:iid=1'

    instance_ = TableauServerManager(login=login, password=passwd, host=host)
    # No need to call update_all_views(); export resolves the workbook directly.
    res, file_path = instance_.export_view(example_url, format='png')
    print(res)
