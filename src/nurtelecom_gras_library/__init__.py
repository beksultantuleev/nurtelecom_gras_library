# from PLSQL_data_importer import PLSQL_data_importer
# from additional_functions import *
# from PLSQL_geodata_importer import PLSQL_geodata_importer
from importlib.metadata import version
__version__ = version("nurtelecom_gras_library")

from core.additional_functions import *
from core.PLSQL_data_importer import PLSQL_data_importer