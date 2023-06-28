import utils.constant as constant
import pandas as pd
import os
import sys
import utils.alert as alert
import pymssql
import json

from datetime import datetime,date, timedelta
from sqlalchemy import create_engine,text,engine
from influxdb import InfluxDBClient
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class PREPARE:


    def __init__(self,server,database,user_login,password,table,table_columns,table_log,table_columns_log,line_notify_token,influx_server,influx_database,influx_user_login,influx_password,column_names,mqtt_topic):
        self.server = server
        self.database = database
        self.user_login = user_login
        self.password = password
        self.table_log = table_log
        self.table = table
        self.table_columns = table_columns
        self.table_columns_log = table_columns_log
        self.df = None
        self.df_insert = None
        self.line_notify_token = line_notify_token
        self.influx_server = influx_server
        self.influx_database = influx_database
        self.influx_user_login = influx_user_login
        self.influx_password = influx_password
        self.column_names = column_names
        self.mqtt_topic = mqtt_topic

    def stamp_time(self):
        now = datetime.now()
        print("\nHi this is job run at -- %s"%(now.strftime("%Y-%m-%d %H:%M:%S")))

    def check_table(self):
        #connect to db
        cnxn,cursor=self.conn_sql()
        # create table
        try:
            cursor.execute('''
            CREATE TABLE '''+self.table+''' (
                '''+self.table_columns+'''
                )
                ''')
            cnxn.commit()
            cursor.close()
            self.info_msg(self.check_table.__name__,f"create a {self.table_log} table successfully" )
        except Exception as e:
            if 'There is already an object named' in str(e):
                self.info_msg(self.check_table.__name__,f"found a {self.table} table" )
            elif 'Column, parameter, or variable' in str(e):
                self.error_msg(self.check_table.__name__,"define columns mistake" ,e)
            else:
                self.error_msg(self.check_table.__name__,"unknow cannot create table" ,e)

    def check_table_log(self):
        #connect to db
        cnxn,cursor=self.conn_sql()
        # create table
        try:
            cursor.execute('''
            CREATE TABLE '''+self.table_log+''' (
                '''+self.table_columns_log+'''
                )
                ''')
            cnxn.commit()
            cursor.close()
            self.info_msg(self.check_table_log.__name__,f"create a {self.table_log} table successfully" )
        except Exception as e:
            if 'There is already an object named' in str(e):
                self.info_msg(self.check_table_log.__name__,f"found a {self.table} table" )
            elif 'Column, parameter, or variable' in str(e):
                self.error_msg(self.check_table_log.__name__,"define columns log mistake" ,e)
            else:
                self.error_msg(self.check_table_log.__name__,"unknow cannot create table log" ,e)

    def error_msg(self,process,msg,e):
        result = {"status":constant.STATUS_ERROR,"process":process,"message":msg,"error":e}
      
        try:
            self.alert_line(self.alert_error_msg(result))
            self.log_to_db(result)
            sys.exit()
        except Exception as e:
            self.info_msg(self.error_msg.__name__,e)
            sys.exit()
    
    def alert_line(self,msg):
        value = alert.line_notify(self.line_notify_token,msg)
        value = json.loads(value)  
        if value["message"] == constant.STATUS_OK:
            self.info_msg(self.alert_line.__name__,'send msg to line notify')
        else:
            self.info_msg(self.alert_line.__name__,value)

    def alert_error_msg(self,result):
        if self.line_notify_token != None:
            return f'\nproject: {self.table}\nprocess: {result["process"]}\nmessage: {result["message"]}\nerror: {result["error"]}\n'
                
    def info_msg(self,process,msg):
        result = {"status":constant.STATUS_INFO,"process":process,"message":msg,"error":"-"}
        print(result)

    def ok_msg(self,process):
        result = {"status":constant.STATUS_OK,"process":process,"message":"program running done","error":"-"}
        try:
            self.log_to_db(result)
            print(result)
        except Exception as e:
            self.error_msg(self.ok_msg.__name__,'cannot ok msg to log',e)
    
    def conn_sql(self):
        #connect to db
        try:
            cnxn = pymssql.connect(self.server, self.user_login, self.password, self.database)
            cursor = cnxn.cursor()
            return cnxn,cursor
        except Exception as e:
            self.alert_line("Danger! cannot connect sql server")
            self.info_msg(self.conn_sql.__name__,e)
            sys.exit()

    def log_to_db(self,result):
        #connect to db
        cnxn,cursor=self.conn_sql()
        try:
            cursor.execute(f"""
                INSERT INTO [{self.database}].[dbo].[{self.table_log}] 
                values(
                    getdate(), 
                    '{result["status"]}', 
                    '{result["process"]}', 
                    '{result["message"]}', 
                    '{result["error"]}'
                    )
                    """
                )
            cnxn.commit()
            cursor.close()
        except Exception as e:
            self.alert_line("Danger! cannot insert log table")
            self.info_msg(self.log_to_db.__name__,e)
            sys.exit()
 

class INFLUX_TO_SQLSERVER(PREPARE):
    def __init__(self,server,database,user_login,password,table,table_columns,table_log,table_columns_log,influx_server,influx_database,influx_user_login,influx_password,column_names,mqtt_topic,line_notify_token=None):
        super().__init__(server,database,user_login,password,table,table_columns,table_log,table_columns_log,line_notify_token,influx_server,influx_database,influx_user_login,influx_password,column_names,mqtt_topic)        

    def lastone(self) :
      try:
          result_lists = []
          client = InfluxDBClient(self.influx_server, 8086, self.influx_user_login,self.influx_password, self.influx_database)
          for i in range(len(self.mqtt_topic)):
              query = f"select time,topic,{self.column_names} from mqtt_consumer where topic = '{self.mqtt_topic[i]}' order by time desc limit 1"
              result = client.query(query)
              if list(result):
                result = list(result)[0][0]
                result_lists.append(result)
                result_df = pd.DataFrame.from_dict(result_lists)
                self.df = result_df
      except Exception as e:
          self.error_msg(self.lastone.__name__,"cannot query influxdb",e)
    
    def edit_col(self):
        try:
            df = self.df.copy()
            df_split = df['topic'].str.split('/', expand=True)
            df['mc_no'] = df_split[2].values
            df['process'] = df_split[1].values
            df.drop(columns=['topic'],inplace=True)
            df.rename(columns = {'time':'data_timestamp'}, inplace = True)
            df["data_timestamp"] =   pd.to_datetime(df["data_timestamp"]).dt.tz_convert(None)    
            df["data_timestamp"] = df['data_timestamp'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
            self.df_insert = df
        except Exception as e:
            self.error_msg(self.edit_col.__name__,"cannot edit dataframe data",e)

    def df_to_db(self):
        #connect to db
        cnxn,cursor=self.conn_sql()
        try:
            df = self.df_insert
            for index, row in df.iterrows():
                cursor.execute(f"""
                INSERT INTO [{self.database}].[dbo].[{self.table}] 
                values(
                    getdate(), 
                    '{row.data_timestamp}',
                    '{row.my_str}',
                    '{row.temp2}',
                    '{row.tmp1}',
                    '{row.mc_no}',
                    '{row.process}'
                    )
                    """
                )

            cnxn.commit()
            cursor.close()
            self.df_insert = None   
            self.info_msg(self.df_to_db.__name__,f"insert data successfully")        
        except Exception as e:
            self.error_msg(self.df_to_db.__name__,"cannot insert df to sql",e)

    def run(self):
        self.stamp_time()
        self.check_table()
        self.check_table_log()
        self.lastone()
        self.edit_col()
        self.df_to_db()
        self.ok_msg(self.df_to_db.__name__)

if __name__ == "__main__":
    print("must be run with main")
