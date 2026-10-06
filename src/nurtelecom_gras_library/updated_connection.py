from nurtelecom_gras_library.OracleDataRetriever import OracleDataRetriever
from nurtelecom_gras_library.OracleGeoDataImporter import OracleGeoDataImporter
from nurtelecom_gras_library.additional_functions import (
    get_all_cred_dict,
    get_all_cred_dict_vso,
)


def get_db_connection(user, database, all_cred_dict=None, geodata=False,
                      vso_paths=None):
    """
    Returns a database connection object for the specified user and database.
    If geodata is True, returns an OracleGeoDataImporter, otherwise OracleDataRetriever.

    Credentials source: ``all_cred_dict`` if given; else the VSO-mounted
    directories in ``vso_paths`` (see :func:`get_all_cred_dict_vso`); else
    Vault via :func:`get_all_cred_dict`.

    The connection uses ``{DATABASE}_DSN`` (``'<host>:<port>/<service_name>'``)
    when present, otherwise ``{DATABASE}_IP``, ``{DATABASE}_PORT`` and
    ``{DATABASE}_SERVICE_NAME``.
    """
    user = user.upper()
    database = database.upper()
    if all_cred_dict is None:
        if vso_paths is not None:
            all_cred_dict = get_all_cred_dict_vso(vso_paths)
        else:
            all_cred_dict = get_all_cred_dict()

    connection_class = OracleGeoDataImporter if geodata else OracleDataRetriever
    try:
        password = all_cred_dict[f'{user}_{database}']
        dsn = all_cred_dict.get(f'{database}_DSN')
        if dsn:
            return connection_class(user=user, password=password, dsn=dsn)
        host = all_cred_dict[f'{database}_IP']
        service_name = all_cred_dict[f'{database}_SERVICE_NAME']
        port = all_cred_dict[f'{database}_PORT']
    except KeyError as e:
        raise ValueError(f"Missing credential for {e.args[0]}") from e

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
