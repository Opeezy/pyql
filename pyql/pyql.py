import pyodbc
import pandas as pd
import traceback
import logging

from typing import Optional, List
from pandas import DataFrame

logging.basicConfig(level=logging.DEBUG)
 
class SqlConn():
    def __init__(self, driver:str, server:str, db:str, user:str, pw:str) -> None:
        self.driver:str = driver
        self.server:str = server
        self.db:str = db
        self.user:str = user
        self.pw:str = pw

    def __str__(self) -> str:
        return f'Connection info:\nDriver: {self.driver}\nServer: {self.server}\nDatabase: {self.db}\nUser: {self.user}\nPassword: {self.pw}'
    
    def send_query(self, query) -> DataFrame:
        try:
            df = DataFrame
            cnxn = pyodbc.connect(f'DRIVER={self.driver};SERVER={self.server};DATABASE={self.db};UID={self.user};PWD={self.pw}')
            cnxn.setencoding(encoding='utf-8')
            df = pd.read_sql_query(query, cnxn)
            cnxn.close()        
            return df
        except Exception as e:
            print(traceback.format_exc())
   

class Select:
    def __init__(self, conn:SqlConn, table:str, columns:List[str]) -> None:
        self.conn = conn
        if columns == []:
            raise Exception("Empty list provided for columns")
        
        column_string = ''
        for c in columns:
            column_string += f'{c}, '

        column_string = column_string.strip().rstrip(',')
        self.query:str = f'Select {column_string} From {table}'

    def to_frame(self) -> DataFrame:
        return self.conn.send_query(self.query)
 


if __name__ == "__main__":
    pass