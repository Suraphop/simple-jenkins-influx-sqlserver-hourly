# data irr
IRR_TABLE = 'data_irr'
IRR_TABLE_COLUMNS ='''
            registered_at datetime,
            data_timestamp datetime,
            my_str varchar(20),
			temp2 varchar(10),
            tmp1 varchar(10),
            mc_no varchar(10),
            process varchar(10),
            '''
            
IRR_TABLE_LOG = 'log_irr'
IRR_TABLE_COLUMNS_LOG ='''
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


MQTT_TOPIC = ['nat/GD/IRR01',
'nat/GD/IRR02',
'nat/GD/IRR03',
'nat/GD/IRR04',
]

COLUMN_NAMES = 'my_str,temp2,tmp1'