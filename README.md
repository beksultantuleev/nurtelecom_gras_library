# nurtelecom_gras_library
This is official NurTelecom GRAS library


from nurtelecom_gras_library import PLSQL_data_importer, etc..

database_connection = PLSQL_data_importer(user = 'test', password = 'testPSWD', host= 'xx.xx.xx.xx', port = 'x')

database_connection.get_data(query = 'select 1 from dual')
