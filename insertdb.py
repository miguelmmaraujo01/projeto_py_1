import connect as conexao
import log
#import sys

def insertDataBase(lista_array, query_insert, name_table):
    arrayValues = []
    for i in lista_array:
        arrayValues.append(i)
        #print(arrayValues)
    conn = conexao.connect_db().oracle
    try:
        cursor = conn.cursor() 
        cursor.prepare(query_insert)
        cursor.executemany(None, arrayValues)

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as err:
        #print(arrayValues)
        log.logger.error(f'{err} - {name_table}')
        #conn.commit()
        cursor.close()
        conn.close()
        raise err