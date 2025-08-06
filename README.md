# nurtelecom_gras_library

Official Python library for interacting with NurTelecom GRAS databases.

## Features

- Secure credential management via Vault
- Easy database connection and querying
- Generate SQL table creation queries from pandas DataFrames

## Installation

```bash
pip install nurtelecom_gras_library
```

## Usage

### Modern Connection (Recommended)

```python
from nurtelecom_gras_library import get_db_connection, get_all_cred_dict, make_table_query_from_pandas

# Retrieve credentials from Vault
all_cred_dict = get_all_cred_dict(
    vault_url=url,
    vault_token=token,
    path_to_secret='path_to_secret',
    mount_point='mount_point'
)

# Create a database connection
database_connection = get_db_connection('login', 'database', all_cred_dict)

# Run a query and get results as a pandas DataFrame
test_query = "select 1 from dual"
test_data = database_connection.get_data(query=test_query)

# Generate a CREATE TABLE SQL statement from the DataFrame
new_table_name = "test_table_name"
create_table_query = make_table_query_from_pandas(df=test_data, table_name=new_table_name)

# Execute the CREATE TABLE statement
database_connection.execute(create_table_query)
```

### Legacy Connection

```python
from nurtelecom_gras_library import PLSQL_data_importer, make_table_query_from_pandas

# Create a legacy database connection
database_connection = PLSQL_data_importer(
    user='user',
    password='pass',
    host='192.168.1.1',
    port='1521'
)

# Run a query and get results as a pandas DataFrame
test_query = "select 1 from dual"
test_data = database_connection.get_data(query=test_query)

# Generate a CREATE TABLE SQL statement from the DataFrame
new_table_name = "test_table_name"
create_table_query = make_table_query_from_pandas(df=test_data, table_name=new_table_name)

# Execute the CREATE TABLE statement
database_connection.execute(create_table_query)
```

## License

MIT License

## Contact

For questions or support, please contact the NurTelecom GRAS team.
