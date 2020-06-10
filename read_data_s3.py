import sys
from io import StringIO
import connect as conexao
import datetimezone as datetime
import log

def busca_arquivo_s3(identificador_arquivo):
    date = '2020-05-04' #datetime.date_time_zn().data
    bucket_name = 'meu-time-qa'
    object_key = f'repositorio/{identificador_arquivo}-{date}.csv'
    #print(object_key)
    conn_s3 = conexao.connect_s3().s3_client
    #body = None
    try:
        log.logger.info('extract datasets from S3....')
        csv_obj = conn_s3.get_object(Bucket=bucket_name, Key=object_key)
        #print(csv_obj)
    except Exception as err:
        log.logger.error(err)
        raise err


    #le arquivo s3 porem lento
    body = csv_obj['Body']
    #print(body)
    csv_string = body.read().decode('utf-8')
    dfpag =  StringIO(csv_string)
    return dfpag