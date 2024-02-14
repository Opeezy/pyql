import pyodbc
import pandas as pd
import traceback
 
class SQL():
    def __init__(self, args: list) -> None:
        self.driver = args[0]
        self.server = args[1]
        self.db = args[2]
        self.user = args[3]
        self.pw = args[4]

    def send_query(self, query) -> None:
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