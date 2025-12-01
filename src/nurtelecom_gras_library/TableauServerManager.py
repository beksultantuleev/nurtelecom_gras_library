import tableauserverclient as TSC
import pandas as pd
import re
import os
# from nurtelecom_gras_library import pass_decoder
from datetime import datetime


class TableauServerManager:
    def __init__(self, login, password,
                 host: str,
                 server_version='3.9',
                 proxies: dict = {}) -> None:
        self.login = login
        self.password = password
        self.host = host.lower()
        self.host_https = self.host.replace('http://', 'https://')
        self.server_version = server_version
        self.use_ssl = True
        self.proxies = proxies

        'establish connection'
        self.tableau_auth = TSC.TableauAuth(self.login, self.password)
        self.server = TSC.Server(self.host, http_options={
                                 "verify": self.use_ssl, 'proxies': self.proxies})
        self.server.version = self.server_version

        self.all_views_raw = []
        self.processed_df = None
        self.accepted_formats_to_export = ['png', 'pdf', 'csv', 'xlsx']

        self.exported_files_folder = 'exported_files'

        self.create_folder(self.exported_files_folder)

    def create_folder(self, folder_path):
        folder_created = False
        try:
            # Create target directory
            os.makedirs(folder_path, exist_ok=True)
            # print(f"Directory '{folder_path}' created successfully")
            folder_created = True
        except OSError as e:
            # Handle the error in case of any other issues except directory already exists
            print(f"Creation of the directory {folder_path} failed due to {e}")
            raise Exception
        if not folder_created:
            print(f"Directory '{folder_path}' already exists")

# Example usage

    def update_all_views(self):
        'get all view as is'
        self.all_views_raw = []
        # Sign in to the server
        with self.server.auth.sign_in(self.tableau_auth):
            req_option = TSC.RequestOptions()
            req_option.page_size = 100  # Adjust this to a reasonable number, e.g., 100

            # all_views = []
            current_page = 1

            while True:
                req_option.pagenumber = current_page
                try:
                    views, pagination_item = self.server.views.get(req_option)
                    if not views:  # If no views are returned, stop the loop
                        break
                    self.all_views_raw.extend(views)

                    # Output views from the current batch
                    for index, view in enumerate(views):
                        # print(f"{index}) {view.name} | Url: {view.content_url}")
                        pass

                    if current_page * req_option.page_size >= pagination_item.total_available:
                        # print(f'total available is {pagination_item.total_available}')
                        break  # Stop if the current page would exceed the number of available items

                    current_page += 1  # Go to the next page
                except TSC.ServerResponseError as e:
                    print(f"Failed to fetch page {current_page}: {e}")
                    break

        # Sign out from the server when done
        self.server.auth.sign_out()

    def make_standart_url(self, old_url):
        'uses only https, not http'
        pattern_for_sheets = "/sheets"
        modified_url = re.sub(pattern_for_sheets, "", old_url)
        new_url = f"{self.host_https}/#/views/{modified_url}"
        return new_url

    def compile_processed_df(self):
        'create pandas df with fixed url (id, name, url)'
        data = []
        for view in self.all_views_raw:
            data.append({
                'ID':       view.id,
                'Name':     view.name,
                'URL':      self.make_standart_url(view.content_url)
            })
            # print(f"{view.name} | id:  {view.id} | Url:  {view.content_url}")
        self.processed_df = pd.DataFrame(data)

    def export_view(self, url, format='png'):
        '''
        accepted formats: png, pdf, csv, xlsx
        '''
        format = format.lower()
        export_format_check = False
        if format in self.accepted_formats_to_export:
            export_format_check = True
        'it should work only with https'
        if 'https://' not in url:
            url = url.replace('http://', 'https://')
        pattern_for_iid = r'\?:iid=\d+'
        # Replace the found pattern with an empty string
        url = re.sub(pattern_for_iid, '', url)
        with self.server.auth.sign_in(self.tableau_auth):
            view_to_export = next((view for view in self.all_views_raw if self.make_standart_url(
                view.content_url) == url), None)
            if export_format_check:
                if view_to_export:
                    try:
                        file_path = f'{self.exported_files_folder}/{view_to_export.name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.{format}'
                        if format == 'png':
                            self.server.views.populate_image(view_to_export)
                            with open(file_path, 'wb') as file:
                                file.write(view_to_export.image)
                            # print(f"Exported as '{view_to_export.name}.{format}'")
                        elif format == 'pdf':
                            self.server.views.populate_pdf(view_to_export)
                            with open(file_path, 'wb') as file:
                                file.write(view_to_export.pdf)
                        elif format == 'csv':
                            self.server.views.populate_csv(view_to_export)
                            if hasattr(view_to_export, 'csv'):
                                with open(file_path, 'wb') as file:
                                    for data in view_to_export.csv:  # view_to_export.csv is a generator
                                        file.write(data)
                        elif format == 'xlsx':
                            self.server.views.populate_excel(view_to_export)
                            # if hasattr(view_to_export, 'xlsx'):
                            with open(file_path, 'wb') as file:
                                for data in view_to_export.excel:  # view_to_export.csv is a generator
                                    file.write(data)
                    except Exception as e:
                        return f'{e}', None
                else:
                    print(f'View does not exist at {url}', None)
                    return f'View does not exist at {url}', None
            else:
                return f'No such format as {format}!', None

        self.server.auth.sign_out()
        return f"SUCCESS! {url} exported as {view_to_export.name}.{format}", file_path


if __name__ == "__main__":
    login = 'xx'
    passwd = 'xx'
    example_url = 'https://tableau.com/#/views/Regional/Economy'
    instance_ = TableauServerManager(login=login,
                                       password=passwd)
    instance_.update_all_views()
    res = instance_.export_view(
        example_url, format='png')

