# nurtelecom_gras_library
This is official NurTelecom GRAS library

```ruby
from nurtelecom_gras_library import get_db_connection, get_all_cred_dict, make_table_query_from_pandas

all_cred_dict = get_all_cred_dict(
    vault_url=url, vault_token=token, path_to_secret='path_to_secret', mount_point='mount_point')


#create connection
database_connection = get_db_connection('login', 'database', all_cred_dict)

#create query
test_query = "select 1 from dual"

#makes pandas df 
test_data = database_connection.get_data(query = test_query)

#make new_create query from pandas df
new_table_name_for_sql = "test_table_name"
query_from_tabel = make_table_query_from_pandas(df = test_data, table_name= new_table_name_for_sql)

#execute query
database_connection.execute(query_from_tabel)


```

Old connection 
```ruby
from nurtelecom_gras_library import PLSQL_data_importer, make_table_query_from_pandas

#create connection
database_connection = PLSQL_data_importer(user = 'user', password = 'pass', host= '192.168.1.1', port = '1521')

#create query
test_query = "select 1 from dual"

#makes pandas df 
test_data = database_connection.get_data(query = test_query)

#make new_create query from pandas df
new_table_name_for_sql = "test_table_name"
query_from_tabel = make_table_query_from_pandas(df = test_data, table_name= new_table_name_for_sql)

#execute query
database_connection.execute(query_from_tabel)


```



