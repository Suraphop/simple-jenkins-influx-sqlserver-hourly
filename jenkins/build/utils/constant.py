# data
TABLE = 'data_demo'
TABLE_COLUMNS ='''
            registered_at datetime,
            data_timestamp datetime,
            D228 varchar(20),
	        D238 varchar(10),
            D250 varchar(10),
            mc_no varchar(10),
            process varchar(10)
            '''
            
TABLE_LOG = 'log_demo'
TABLE_COLUMNS_LOG ='''
            registered_at datetime,
	        status varchar(50),
            process varchar(50),
            message varchar(50),
            error varchar(MAX),
            '''

#LOG status
STATUS_OK = 'ok'
STATUS_ERROR = 'error'
STATUS_INFO = 'info'


MQTT_TOPIC = ['mic/test/BM165_146']

COLUMN_NAMES = 'D228,D238,D250'