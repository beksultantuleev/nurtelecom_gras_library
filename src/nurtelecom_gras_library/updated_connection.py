from nurtelecom_gras_library.OracleDataRetriever import OracleDataRetriever
from nurtelecom_gras_library.OracleGeoDataImporter import OracleGeoDataImporter
from nurtelecom_gras_library.additional_functions import *


def get_db_connection(user, database, all_cred_dict=None, geodata=False):
    """
    Returns a database connection object for the specified user and database.
    If geodata is True, returns an OracleGeoDataImporter, otherwise OracleDataRetriever.
    """
    user = user.upper()
    database = database.upper()
    if all_cred_dict is None:
        all_cred_dict = get_all_cred_dict()

    try:
        password = all_cred_dict[f'{user}_{database}']
        host = all_cred_dict[f'{database}_IP']
        service_name = all_cred_dict[f'{database}_SERVICE_NAME']
        port = all_cred_dict[f'{database}_PORT']
    except KeyError as e:
        raise ValueError(f"Missing credential for {e.args[0]}") from e

    connection_class = OracleGeoDataImporter if geodata else OracleDataRetriever
    return connection_class(
        user=user,
        password=password,
        host=host,
        service_name=service_name,
        port=port,
    )

if __name__ == "__main__":
    database_connection = get_db_connection('xx', 'xx')
    pass
