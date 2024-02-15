import pyodbc
import pandas as pd
import traceback
import logging
import warnings
warnings.filterwarnings('ignore')

from typing import Optional, List, Dict, Union
from pandas import DataFrame
from binascii import hexlify

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
            cnxn = pyodbc.connect(f'DRIVER={self.driver};SERVER={self.server};DATABASE={self.db};UID={self.user};PWD={self.pw}')
            cnxn.setencoding(encoding='utf-8')
            cnxn.add_output_converter(-151, self.__handle_geometry)
            df = pd.read_sql_query(query, cnxn)
            cnxn.close()        
            return df
        except Exception as e:
            print(traceback.format_exc())
    
    def get_all_columns(self, table) -> List[str]:
        columns = []
        cnxn = pyodbc.connect(f'DRIVER={self.driver};SERVER={self.server};DATABASE={self.db};UID={self.user};PWD={self.pw}')
        cnxn.setencoding(encoding='utf-8')
        cnxn.add_output_converter(-151, self.__handle_geometry)
        cursor = cnxn.cursor()
        for row in cursor.columns(table=table):
            columns.append(row.column_name)
        cnxn.close()     
        return columns 

    def __handle_geometry(self, geom):
        return f"0x{hexlify(geom).decode().upper()}"
   

class Select:
    def __init__(self,
                  conn:SqlConn,
                    table:str,
                      columns:Optional[List[str]] = None,
                        where: Optional[Dict[str, Union[str,int]]] = None) -> None:
        self.conn = conn
        self.__table = table
        self.__column_string = ''
        self.query = f'Select {self.__column_string} From {self.__table}'
        if columns is None:
            col = self.conn.get_all_columns(table)
            self.__build_column_string(col)
        else:        
            self.__build_column_string(columns)
        
        if where is not None:
            self.query += ' Where '
            for key, value in where.items():
                self.query += f'{key} = {value} And '
            self.query = self.query.rstrip(' And ')
        logging.debug(self.query)

    def __str__(self) -> str:
        return self.query

    def to_frame(self) -> DataFrame:
        return self.conn.send_query(self.query)
    
    def __build_column_string(self, columns):
        for c in columns:
            self.__column_string += f'{c}, '
        self.__column_string = self.__column_string.strip().rstrip(',')
        self.query = f'Select {self.__column_string} From {self.__table}'
    
 


if __name__ == "__main__":
    pass