import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()


def connect_database():
    try:
        wfh_db = mysql.connector.connect(
            host=os.getenv('host'),
            user=os.getenv('user'),
            passwd=os.getenv('passwd'),
            database=os.getenv('database'),
            auth_plugin=os.getenv('auth_plugin')
        )
        return wfh_db
    except Exception as e:
        print('An error happed: ' + str(e))
        return None
