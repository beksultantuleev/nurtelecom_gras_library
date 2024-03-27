from nurtelecom_gras_library.PLSQL_data_importer import PLSQL_data_importer
from nurtelecom_gras_library.PLSQL_geodata_importer import PLSQL_geodata_importer
from nurtelecom_gras_library.additional_functions import *

def get_db_connection(user, database, geodata = False):
    database = database.upper()
    user = user.upper()
    database_connection = PLSQL_data_importer(user=user,
                                              password=pass_decoder(
                                                  os.environ.get(f'{user}_{database}')),
                                              host=pass_decoder(
                                                  os.environ.get(f'{database}_IP')),
                                              service_name=pass_decoder(
                                                  os.environ.get(f'{database}_SERVICE_NAME')),
                                              port=pass_decoder(
                                                  os.environ.get(f'{database}_PORT')),
                                              )
    if geodata:
        database_connection = PLSQL_geodata_importer(user=user,
                                              password=pass_decoder(
                                                  os.environ.get(f'{user}_{database}')),
                                              host=pass_decoder(
                                                  os.environ.get(f'{database}_IP')),
                                              service_name=pass_decoder(
                                                  os.environ.get(f'{database}_SERVICE_NAME')),
                                              port=pass_decoder(
                                                  os.environ.get(f'{database}_PORT')),
                                              )

    return database_connection

if __name__ == "__main__":
    database_connection = get_db_connection('kpi', 'dwh_sd')
    pass