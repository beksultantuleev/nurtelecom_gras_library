from nurtelecom_gras_library.PLSQL_data_importer import PLSQL_data_importer
from nurtelecom_gras_library.PLSQL_geodata_importer import PLSQL_geodata_importer
from nurtelecom_gras_library.additional_functions import *


def get_db_connection(user, database, all_cred_dict=None, geodata=False):
    user = user.upper()
    database = database.upper()
    if all_cred_dict:
        if geodata:
            database_connection = PLSQL_geodata_importer(user=user,
                                                  password=all_cred_dict[f'{user}_{database}'],
                                                  host=all_cred_dict[f'{database}_IP'],
                                                  service_name=all_cred_dict[f'{database}_SERVICE_NAME'],
                                                  port=all_cred_dict[f'{database}_PORT'],
                                                  )
        database_connection = PLSQL_data_importer(user=user,
                                                  password=all_cred_dict[f'{user}_{database}'],
                                                  host=all_cred_dict[f'{database}_IP'],
                                                  service_name=all_cred_dict[f'{database}_SERVICE_NAME'],
                                                  port=all_cred_dict[f'{database}_PORT'],
                                                  )
        return database_connection
    'for legacy connection. will be removed later'
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
