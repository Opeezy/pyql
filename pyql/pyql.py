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
                columns:Optional[List[str]] = None,
                where: Optional[Dict[str, List]] = None,
                order: Optional[List[str]] = None) -> None:
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
        if where is not None:
            self.__build_where(where)
        if order is not None:
            self.__build_order(order)
            
        self.query = f'Select{self.__top}{self.__column_string} From {self.__table}{self.__where}{self.__order}'

    def __str__(self) -> str:
        return self.query

    def to_frame(self) -> DataFrame:
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

    def __build_where(self, where:Dict[str, Union[str,int]]):
        self.__where = ' Where'
        for key, value in where.items():

            if type(value[1]) == str:
                value[1] = f"'{value[1]}'"
            self.__where += f" {key} {value[0]} {value[1]} And"
        self.__where = self.__where.rstrip(' And ')

    def __build_order(self, order:List[str]):
        self.__order = ' Order By '
        for o in order:
            self.__order += f'{o}, '
        self.__order = self.__order.rstrip(', ')
    
    def __col_types(self, col):
        d = {'column': col, 'types': self.__types}
        df = pd.DataFrame(data=d)
        print(df)

def get_drivers():
    d = pyodbc.drivers()
    df = pd.DataFrame(data=d, columns=['Drivers'])
    print(df)

if __name__ == "__main__":
    pass