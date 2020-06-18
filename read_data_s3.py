import sys
from io import StringIO
import connect as conexao
import datetimezone as datetime
import log
from botocore.exceptions import NoCredentialsError 
#import pyarrow

def busca_arquivo_s3(identificador_arquivo):
    date = '2020-05-04' #datetime.date_time_zn().data
    bucket_name = 'meu-time-qa' #nomedobucker
    object_key = f'repositorio/{identificador_arquivo}-{date}.csv' #alterar o nome do repositorio
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


def busca_arquivo_s3(identificador_arquivo):
    date = '2020-05-04' #datetime.date_time_zn().data
    bucket_name = 'pagseguro-akron-qa'
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


def envia_arquivo_s3(param_df, identificador_arquivo): #carregar bucket akron - dataframe tratado na etapa1
    s3_resource = conexao.connect_s3().s3_client_akron_upload.resource('s3')

    try:
        date = '2020-05-04' #datetime.date_time_zn().data
        bucket_name = 'pagseguro-akron-qa'
        #object_key = f'upload_arq/upload_{identificador_arquivo}-{date}.snappy.parquet'      #gerar arquivo parquet estou com problema lib windows local
        object_key = f'upload_arq/upload_{identificador_arquivo}-{date}.csv'
        csv_buffer = StringIO()
        param_df.to_csv(csv_buffer, sep=";", index=False)
        #param_df.to_parquet(csv_buffer, engine='pyarrow', compression='snappy') #gerar arquivo parquet estou com problema lib windows local
        s3_resource.Object(bucket_name, object_key).put(Body=csv_buffer.getvalue())

        log.logger.info('Upload success to S3 ...')
        return True
    except FileNotFoundError:
        log.logger.info("The file was not found")
        return False
    except NoCredentialsError:
        log.logger.info("Credentials not available")
        return False
        #print(csv_obj)
    except Exception as err:
        log.logger.error(err)
