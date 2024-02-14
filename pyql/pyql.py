import pyodbc
import pandas as pd
import traceback
import logging

from typing import Optional, List
from pandas import DataFrame

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)
 
class Pyql():
    def __init__(self, driver:str, server:str, db:str, user:str, pw:str) -> None:
        self.__driver:str = driver
        self.__server:str = server
        self.__db:str = db
        self.__user:str = user
        self.__pw:str = pw

    def __str__(self) -> str:
        return f'Connection info:\nDriver: {self._driver}\nServer: {self._server}\nDatabase: {self._db}\nUser: {self._user}\nPassword: {self._pw}'
    
    def __send_query(self, query) -> DataFrame:
        try:
            cnxn = pyodbc.connect(f'DRIVER={self.__driver};SERVER={self.__server};DATABASE={self.__db};UID={self.__user};PWD={self.__pw}')
            cnxn.setencoding(encoding='utf-8')
            cursor = cnxn.cursor()
            df = pd.read_sql_query(query, cnxn)
            cnxn.close()        
            return df
        except Exception as e:
            print(traceback.format_exc())

    def select(self, table:str, args: Optional[List[str]] = None) -> DataFrame:
        columns = ''

        if args is None:
            columns = '*'
        else:
            for arg in args:
                columns += f'{arg}, '

        columns = columns.strip().rstrip(',')
        query = f'''Select {columns} From {table}'''

        return query

            


    def _send_query(self, query) -> None:
        try:
            cnxn = pyodbc.connect(f'DRIVER={self.driver};SERVER={self.server};DATABASE={self.db};UID={self.user};PWD={self.pw}')
            cnxn.setencoding(encoding='utf-8')
            cursor = cnxn.cursor()
            df = pd.read_sql_query(query, cnxn)
            cnxn.close()        
            return df
        except Exception as e:
            return traceback.format_exc()

if __name__ == "__main__":
    pass