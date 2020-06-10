import cx_Oracle
#import psycopg2
#import sys
#import os
import boto3
import log

#oracle precisar ser alterado fontes do lnm lake
conn_info = {
    'host': 'xxx',
    'port': 1521,
    'user': 'dbaad',
    'psw': 'xxx',
    'service': 'xxxx'
}
connstr = '{user}/{psw}@{host}:{port}/{service}'.format(**conn_info)

#conn = cx_Oracle.connect(connstr)
#conn = cx_Oracle.connect('xx/123@xxxx:1521/xxx.bi.xxxx')

#s3
key = 'xxxxx'
secret_key = 'xxxxx'
region = 'us-east-1'


class connect_database():
    def __init__(self):
        #self.postgres = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (db, usr, hst, psw))
        self.s3_client = boto3.client('s3',aws_access_key_id=key ,aws_secret_access_key=secret_key, region_name=region)
        self.oracle = cx_Oracle.connect(connstr)

def connect_db():
    try:
        log.logger.info('Connecting database ...')
        return connect_database()
    except Exception as err:
        log.logger.error(err)

def connect_s3():
    try:
        log.logger.info('Connecting S3 ...')
        return connect_database()
    except Exception as err:
        log.logger.error(err)