import utils.constant as constant
import os

from utils.influx_to_sqlserver import INFLUX_TO_SQLSERVER
from dotenv import load_dotenv

load_dotenv()

influx_to_sqlserver = INFLUX_TO_SQLSERVER(
        server=os.getenv('SERVER'),
        database=os.getenv('DATABASE'),
        user_login=os.getenv('USER_LOGIN'),
        password=os.getenv('PASSWORD'),
        table=constant.IRR_TABLE,
        table_columns=constant.IRR_TABLE_COLUMNS,
        table_log=constant.IRR_TABLE_LOG,
        table_columns_log=constant.IRR_TABLE_COLUMNS_LOG,

        influx_server=os.getenv('INFLUX_SERVER'),
        influx_database=os.getenv('INFLUX_DATABASE'),
        influx_user_login=os.getenv('INFLUX_USER_LOGIN'),
        influx_password=os.getenv('INFLUX_PASSWORD'),
        column_names=constant.COLUMN_NAMES,
        mqtt_topic=constant.MQTT_TOPIC,
        line_notify_token=os.getenv('LINE_NOTIFY_TOKEN'),
    )
influx_to_sqlserver.run()