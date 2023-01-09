import pandas as pd
import datetime
import os
import re
import psycopg2
import pathlib


class SQL_QUERY:
    
    """
    Summary: A class to automate the create table and insert table queries from 
    a dataframe
    """

    def __init__(self):
        pass
        
        

    def create_table_query(self,df,table_name):
        """
        Create table query from a dataframe
        """
        query = [f"""CREATE TABLE IF NOT EXISTS {table_name} (ID  SERIAL PRIMARY KEY,"""]
        for i in range(len(df.columns)):
            text = df.columns[i]
            text=re.sub(r"\?", ".", str(text))
            text=re.sub(r"\!", ".", text)
            text=re.sub(r'([.])\1+', r'\1', text)
            text = re.sub(r"[\([{})\]]", "", text)
            string = "_".join(text.split(" "))
            query.append(f"{string} varchar(255),")
        query.append('time_stamp varchar(100))')
        return "".join(query)
    
    
    def insert_table_query(self,df,table_name):
        """
        Insert table query from a dataframe
        **ONLY USE THIS FUNCTION IF USING create_table_query**
        """
        query = [f"""INSERT INTO {table_name} ("""]
        for i in range(len(df.columns)):
            values = '%s,'*(len(df.columns))
            text = df.columns[i]
            text=re.sub(r"\?", ".", str(text))
            text=re.sub(r"\!", ".", text)
            text=re.sub(r'([.])\1+', r'\1', text)
            text = re.sub(r"[\([{})\]]", "", text)
            string = "_".join(text.split(" "))
            query.append(f"{string},")
        query.append(f'time_stamp) VALUES ({values}%s)')
        return "".join(query)
    
    
                
class SqliteOperations:
    
    def __init__(self):
        pass
    
    
    def connection(self, db_name):
        return sqlite3.connect(db_name)
    
    
    def create_table(self,db_name, create_query):
        conn = self.connection(db_name)
        cur = conn.cursor()
        cur.execute(create_query)
        return conn.commit()
    
    
    def insert_dataset(self,db_name, insert_query,df):
        df['time_stamp'] = datetime.datetime.now()
        df = df.fillna(0)
        df = df.applymap(str)
        conn = self.connection(db_name)
        cur = conn.cursor()
        cur.executemany(insert_query,df.values.tolist())
        conn.commit()
        return conn.close()
    

    def get_all_files(self,path):
        """
        Get all the files in a folder with the suffix of path

        Args:
            path (raw string): Path of folder 

        Returns:
            list: All the file in the path along with the folder suffix
        """
        files_list = os.listdir(path)
        files_list_with_path = [os.path.join(path,file) for file in files_list]
        return files_list_with_path
    
    
    def csv_or_excel(self, file_name):
        ext = pathlib.Path(file_name).suffix
        if ext == '.csv':
            df = pd.read_csv(file_name)
        
        elif ext == '.xls' or ext == '.xlsx':
            df = pd.read_excel(file_name)
        else:
            df =  "File format not supported"
        return df
    
    
    
class PostgresConnect:
    
    def __init__(self):
        pass
    
    def connect(self,db_name,user,host,password):
        conn = psycopg2.connect(f"dbname={db_name} user={user} host={host} password={password}")
        return conn 
        pass
    
    def insert_data(self, db_name,user,host,password,insert_query,df):
        df['time_stamp'] = datetime.datetime.now()
        df = df.fillna(0)
        df = df.applymap(str)
        conn = psycopg2.connect(f"dbname={db_name} user={user} host={host} password={password}")
        cur = conn.cursor()
        cur.executemany(insert_query,df.values.tolist())
        conn.commit()
        return conn.close()
    
    def create_table(self, db_name,user,host,password,create_query):
        conn = psycopg2.connect(f"dbname={db_name} user={user} host={host} password={password}")
        cur = conn.cursor()
        cur.execute(create_query)
        return conn.commit()