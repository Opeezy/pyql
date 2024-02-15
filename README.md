# pyql
A query tool for SQL Server

## Usage
#### First, define your sql connection
```python
import pyql

conn = pyql.SqlConn("<ODBC DRIVER>",
                    "<SERVER>\\<INSTANCE>",
                    "<DATABASE>",
                    "<USERNAME>",
                    "<PASSWORD>")
```
#### If you need an easy way to find your odbc drivers, use...
```python
pyql.get_drivers()
```
#### Building a Select query
```python
p = pyql.Select("Users")
```
```sql
SELECT * FROM USERS
```
#### Adding specific search columns
```python
COLUMNS = [
    'UserID',
    'UserName',
    'Password'
]
p = pyql.Select("Users", columns=COLUMNS)
```
```sql
SELECT UserID, UserName, Password 
FROM USERS
```
#### Adding a where clause
```python
COLUMNS = [
    'UserID',
    'UserName',
    'Password'
]
WHERE = {
    'UserName': ['=','foo bar'] 
}
p = pyql.Select("Users", columns=COLUMNS, where=WHERE)
```
```sql
SELECT UserID, UserName, Password 
FROM USERS 
WHERE UserName = 'foo bar'
```
#### Using different comparison operators
```python
COLUMNS = [
    'UserID',
    'UserName',
    'Password'
]
WHERE = {
    'Age': ['>', 24],
    'Siblings': ['<=', 3],
    'Name': ['Like', 'John%']
}
p = pyql.Select("Users", columns=COLUMNS, where=WHERE)
```
```sql
SELECT UserID, UserName, Password 
FROM USERS 
WHERE Age > 24 And Siblings <= 3 And Name Like 'John%'
```