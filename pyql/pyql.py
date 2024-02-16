import pyodbc
import pandas as pd
import traceback
import logging
import os
import warnings

from typing import Optional, List, Dict, Union
from pandas import DataFrame
from binascii import hexlify
from pyql.exceptions import *

warnings.filterwarnings('ignore')

OPERATORS = [
    '=',
    '>',
    '<',
    '>=',
    '<=',
    'like'
]
 
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
        cnxn = pyodbc.connect(f'DRIVER={self.driver};SERVER={self.server};DATABASE={self.db};UID={self.user};PWD={self.pw}')
        cnxn.setencoding(encoding='utf-8')
        cnxn.add_output_converter(-151, self.__handle_geometry)
        df = pd.read_sql_query(query, cnxn)
        cnxn.close()        
        return df
    
    def get_all_columns(self, table) -> List[str]:
        columns = []
        types = []
        cnxn = pyodbc.connect(f'DRIVER={self.driver};SERVER={self.server};DATABASE={self.db};UID={self.user};PWD={self.pw}')
        cnxn.setencoding(encoding='utf-8')
        cnxn.add_output_converter(-151, self.__handle_geometry)
        cursor = cnxn.cursor()
        for row in cursor.columns(table=table):
            types.append(row[5])
            columns.append(row.column_name)
        cnxn.close()  
        return columns, types

    def __handle_geometry(self, geom) -> str:
        return f"0x{hexlify(geom).decode().upper()}"
   

class Select:
    def __init__(self,
                conn:SqlConn,
                table:str,
                top: Optional[int] = None,
                columns:Optional[List[str]] = None) -> None:
        self.conn = conn
        self.__table = table
        self.__column_string = ''
        self.__top = ''
        self.__where = ''
        self.__order = ''

        if top is not None:
            self.__top = f' top {top}'

        if columns is None:
            col, types = self.conn.get_all_columns(table)
            self.__types = types
            #self.__col_types(col)
            self.__build_column_string(col)
        else:        
            self.__build_column_string(columns)

        self.query = f'Select{self.__top}{self.__column_string} From {self.__table}{self.__where}{self.__order}'

    def __str__(self) -> str:
        return self.query

    def frame(self) -> DataFrame:
        df = self.conn.send_query(self.query)
        if df.empty:
            raise EmptyDataError("Query returned nothing")
        return df
    
    def to_excel(self,
                filename:str,
                sheetname:str = 'NewQuery',
                auto:bool = True) -> None:
        if auto:
                df = self.conn.send_query(self.query)
                df = df.fillna(value='NULL')
                df.to_excel(filename, sheet_name=sheetname)
                logging.info(f"Query saved to '{filename}'")
        else:
            if os.path.exists(filename):
                a = input(f"File '{filename}' already exists. Overwrite?(y/n):")
                a = a.strip().lower()
                if a == 'y':
                    df = self.conn.send_query(self.query)
                    df = df.fillna(value='NULL')
                    df.to_excel(filename, sheet_name=sheetname)
                    logging.info(f"Query saved to '{filename}'")
                else:
                    logging.info('Aborting file save')

    def __build_column_string(self, columns):
        for c in columns:
            self.__column_string += f'{c}, '
        self.__column_string = f' {self.__column_string.strip().rstrip(',')}'
    
    def where(self, operator:str ,values:Dict[str,Union[str,int]]) -> None:
        operator = operator.strip().lower()
        if operator in OPERATORS:            
            if len(values) > 0:
                self.__where = ' Where '
                for key, value in values.items():
                    self.__where += f'{key} {operator} {value} And'
                self.__where = self.__where.rstrip(' And')
                self.query = f'Select{self.__top}{self.__column_string} From {self.__table}{self.__where}{self.__order}'
            return self
        
    def order(self, order: Union[List[str],str]):
        self.__order = ' Order By '
        print(type(order))
        if type(order) is list:
            for o in order:
                self.__order += f'{o}, '
            self.__order = self.__order.rstrip(', ')
            self.query = f'Select{self.__top}{self.__column_string} From {self.__table}{self.__where}{self.__order}'
            return self
        else:
            print("test")
            self.__order += order
            self.query = f'Select{self.__top}{self.__column_string} From {self.__table}{self.__where}{self.__order}'
        return self

    

def get_drivers():
    d = pyodbc.drivers()
    df = pd.DataFrame(data=d, columns=['Drivers'])
    print(df)

if __name__ == "__main__":
    pass