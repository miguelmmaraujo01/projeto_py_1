import pandas as pd
import os
import connect as conexao
import datetimezone as datetime
import teste_drop_create_tb_cart as dctb
import listcases
import insertdb
import log

#startTime = datetime.date_time_zn().now


def create_tb_carteira_status():
    conn = conexao.connect_db().oracle

    dctb.drop_create_tb_cart()  #funcao que cria estrutura de tabelas para buscar carteira do cliente
    sql = dctb.query_select()   #query que utiliza o tratamento com tb do lake para buscar carteira de cliente

    df_list = []
    conn = conexao.connect_db().oracle
    for chunk in pd.read_sql_query(sql , conn, chunksize=1000):
        df_list.append(chunk)
    df_tabela =  pd.concat(df_list, ignore_index=True)

    try:
        log.logger.info('5 - credito.cart_cobr_sts_cart_valor')#print('5 - credito.cart_cobr_sts_cart_valor')

        df_cart_sts_valor = df_tabela.groupby(['CARTEIRA', 'IND_DIVISAO'])['NUM_SALDO_CONTABIL'].sum().to_frame().reset_index()
        df_cart_sts_valor = df_cart_sts_valor.pivot_table(values='NUM_SALDO_CONTABIL', index='CARTEIRA', columns='IND_DIVISAO',aggfunc=sum).fillna(0)

        df_cart_sts_valor = pd.DataFrame(df_cart_sts_valor, columns = ['Capital', 'WhiteLabel', 'totalmr']).fillna(0)

        df_cart_sts_valor['totalmr'] = df_cart_sts_valor.sum(axis = 1)
        df_cart_sts_valor.loc['totalmr'] = df_cart_sts_valor.sum(axis = 0)
            
        lista_array = list(df_cart_sts_valor.to_records(index=True, column_dtypes=dict))
        query_insert = "INSERT /*array */ INTO credito.cart_cobr_sts_cart_valor (status_carteira, capital, whitelabel, totalmr) values (:1, :2, :3, :4)"

        table_name = ("credito.cart_cobr_sts_cart_valor")
        insertdb.insertDataBase(lista_array, query_insert, table_name)
        list.clear(lista_array)

        log.logger.info('6 - credito.cart_cobr_sts_cart_qtd')#print('6 - credito.cart_cobr_sts_cart_qtd')

        df_cart_sts_qtd_op = df_tabela.groupby(['CARTEIRA', 'IND_DIVISAO'])['IDT_OPERACAO'].size().to_frame().reset_index()
        df_cart_sts_qtd_op = df_cart_sts_qtd_op.pivot_table(values='IDT_OPERACAO', index='CARTEIRA', columns='IND_DIVISAO',aggfunc=pd.Series.unique).fillna(0)
        df_cart_sts_qtd_op = pd.DataFrame(df_cart_sts_qtd_op, columns = ['Capital', 'WhiteLabel', 'totalmr']).fillna(0) 

        df_cart_sts_qtd_op['totalmr'] = df_cart_sts_qtd_op.sum(axis = 1)
        df_cart_sts_qtd_op.loc['totalmr'] = df_cart_sts_qtd_op.sum(axis = 0)

        lista_array = list(df_cart_sts_qtd_op.to_records(index=True, column_dtypes=dict))
        query_insert = "INSERT /*array */ INTO credito.cart_cobr_sts_cart_qtd (status_carteira, capital, whitelabel, totalmr) values (:1, :2, :3, :4)"
    
        table_name = ("credito.cart_cobr_sts_cart_qtd")
        insertdb.insertDataBase(lista_array, query_insert, table_name)
        list.clear(lista_array)


        log.logger.info('7 - credito.cart_cobr_sts_cart_porc')#print('7 - credito.cart_cobr_sts_cart_porc')

        df_cart_carteira_op_porc = pd.DataFrame(df_cart_sts_qtd_op, columns = ['Capital',  'WhiteLabel'])  
        df_cart_carteira_op_porc = df_cart_carteira_op_porc.drop('totalmr')

        df_cart_carteira_op_porc['Capital'] = df_cart_carteira_op_porc['Capital']/df_cart_carteira_op_porc['Capital'].sum(axis = 0)*100 
        df_cart_carteira_op_porc['WhiteLabel'] = df_cart_carteira_op_porc['WhiteLabel']/df_cart_carteira_op_porc['WhiteLabel'].sum(axis = 0)*100 

        df_cart_carteira_op_porc = pd.DataFrame(df_cart_carteira_op_porc, columns = ['Capital', 'WhiteLabel', 'totalmr']).fillna(0) 

        df_cart_carteira_op_porc['totalmr'] = df_cart_carteira_op_porc.sum(axis = 1)
        df_cart_carteira_op_porc.loc['totalmr'] = df_cart_carteira_op_porc.sum(axis = 0)


        lista_array = list(df_cart_carteira_op_porc.to_records(index=True, column_dtypes=dict))
        query_insert = "INSERT /*array */ INTO credito.cart_cobr_sts_cart_porc (status_carteira, capital, whitelabel, totalmr) values (:1, :2, :3, :4)"

        table_name = ("credito.cart_cobr_sts_cart_porc")
        insertdb.insertDataBase(lista_array, query_insert, table_name)
        list.clear(lista_array)


        #print('AGING ')
        log.logger.info('8 - credito.cart_cobr_sts_cart_aging_valor')#print('8 - cart_cobr_sts_cart_aging_valor')

        df_tabela['AGING'] = listcases.lista_atraso_dias(df_tabela['NUM_ATRASO_TOTAL_DIAS'])

        df_cart_sts_valor = df_tabela.groupby(['AGING', 'CARTEIRA'])['NUM_SALDO_CONTABIL'].sum().to_frame().reset_index()
        df_cart_carteira_valor = df_cart_sts_valor.pivot_table(values='NUM_SALDO_CONTABIL', index='AGING', columns='CARTEIRA',aggfunc=sum).fillna(0)

        df_cart_carteira_valor = pd.DataFrame(df_cart_carteira_valor, columns = ['0','GERENTEVIRTUAL', 'LONGTAIL', 'PARCERIAS', 'BERCARIO','SUBADQUIRENTE','EMPRESAS','VAREJO', 'ISO']).fillna(0)  

        df_cart_carteira_valor['TOTALMR'] = df_cart_carteira_valor.sum(axis = 1)
        df_cart_carteira_valor.loc['TOTALMR'] = df_cart_carteira_valor.sum(axis = 0)

        lista_array = list(df_cart_carteira_valor.to_records(index=True, column_dtypes=dict))
        query_insert = "INSERT /*array */ INTO credito.cart_cobr_sts_cart_aging_valor (AGING, ZERO, GERENTEVIRTUAL, LONGTAIL, PARCERIAS, BERCARIO,SUBADQUIRENTE,EMPRESAS,VAREJO, ISO, TOTALMR) values (:0, :1, :2, :3, :4, :5, :6, :7, :8, :9, :10)"

        table_name = ("credito.cart_cobr_sts_cart_aging_valor")
        insertdb.insertDataBase(lista_array, query_insert, table_name)
        list.clear(lista_array)


        log.logger.info('9 - credito.cart_cobr_sts_cart_aging_qtd')#print('9 - credito.cart_cobr_sts_cart_aging_qtd')
        df_cart_sts_aging_qtd_op = df_tabela.groupby(['AGING', 'CARTEIRA'])['IDT_OPERACAO'].size().to_frame().reset_index()
        df_cart_sts_aging_qtd_op = df_cart_sts_aging_qtd_op.pivot_table(values='IDT_OPERACAO', index=['AGING'], columns='CARTEIRA',aggfunc=pd.Series.unique).fillna(0)

        df_cart_sts_aging_qtd_op = pd.DataFrame(df_cart_sts_aging_qtd_op, columns = ['0','GERENTEVIRTUAL', 'LONGTAIL', 'PARCERIAS', 'BERCARIO','SUBADQUIRENTE','EMPRESAS','VAREJO', 'ISO']).fillna(0)  

        df_cart_sts_aging_qtd_op['TOTALMR'] = df_cart_sts_aging_qtd_op.sum(axis = 1)
        df_cart_sts_aging_qtd_op.loc['TOTALMR'] = df_cart_sts_aging_qtd_op.sum(axis = 0)

        lista_array = list(df_cart_sts_aging_qtd_op.to_records(index=True, column_dtypes=dict))
        query_insert = "INSERT /*array */ INTO credito.cart_cobr_sts_cart_aging_qtd (AGING, ZERO, GERENTEVIRTUAL, LONGTAIL, PARCERIAS, BERCARIO,SUBADQUIRENTE,EMPRESAS,VAREJO, ISO, TOTALMR)  values (:0, :1, :2, :3, :4, :5, :6, :7, :8, :09 ,:10)"

        table_name = ("credito.cart_cobr_sts_cart_aging_qtd")
        insertdb.insertDataBase(lista_array, query_insert, table_name)
        list.clear(lista_array)


        log.logger.info('10 - credito.cart_cobr_sts_cart_aging_porc')#print('10 - credito.cart_cobr_sts_cart_aging_porc')

        #0  BERÇÁRIO    EMPRESAS    GERENTE VIRTUAL LONGTAIL    PARCERIAS   SUBADQUIRENTE   A DESCOBRIR VAREJO  ISO
        df_cart_sts_aging_qtd_porc = pd.DataFrame(df_cart_sts_aging_qtd_op, columns = ['0','GERENTEVIRTUAL', 'LONGTAIL', 'PARCERIAS', 'BERCARIO','SUBADQUIRENTE','EMPRESAS','VAREJO', 'ISO'])  
        df_cart_sts_aging_qtd_porc = df_cart_sts_aging_qtd_porc.drop('TOTALMR')

        df_cart_sts_aging_qtd_porc['0'] = df_cart_sts_aging_qtd_porc['0']/df_cart_sts_aging_qtd_porc['0'].sum(axis = 0)*100 
        df_cart_sts_aging_qtd_porc['GERENTEVIRTUAL'] = df_cart_sts_aging_qtd_porc['GERENTEVIRTUAL']/df_cart_sts_aging_qtd_porc['GERENTEVIRTUAL'].sum(axis = 0)*100 
        df_cart_sts_aging_qtd_porc['LONGTAIL'] = df_cart_sts_aging_qtd_porc['LONGTAIL']/df_cart_sts_aging_qtd_porc['LONGTAIL'].sum(axis = 0)*100 
        df_cart_sts_aging_qtd_porc['PARCERIAS'] = df_cart_sts_aging_qtd_porc['PARCERIAS']/df_cart_sts_aging_qtd_porc['PARCERIAS'].sum(axis = 0)*100 
        df_cart_sts_aging_qtd_porc['BERCARIO'] = df_cart_sts_aging_qtd_porc['BERCARIO']/df_cart_sts_aging_qtd_porc['BERCARIO'].sum(axis = 0)*100 
        df_cart_sts_aging_qtd_porc['SUBADQUIRENTE'] = df_cart_sts_aging_qtd_porc['SUBADQUIRENTE']/df_cart_sts_aging_qtd_porc['SUBADQUIRENTE'].sum(axis = 0)*100 
        df_cart_sts_aging_qtd_porc['EMPRESAS'] = df_cart_sts_aging_qtd_porc['EMPRESAS']/df_cart_sts_aging_qtd_porc['EMPRESAS'].sum(axis = 0)*100 
        df_cart_sts_aging_qtd_porc['VAREJO'] = df_cart_sts_aging_qtd_porc['VAREJO']/df_cart_sts_aging_qtd_porc['VAREJO'].sum(axis = 0)*100 
        df_cart_sts_aging_qtd_porc['ISO'] = df_cart_sts_aging_qtd_porc['ISO']/df_cart_sts_aging_qtd_porc['ISO'].sum(axis = 0)*100 

        df_cart_sts_aging_qtd_porc = pd.DataFrame(df_cart_sts_aging_qtd_porc, columns = ['0','GERENTEVIRTUAL', 'LONGTAIL', 'PARCERIAS', 'BERCARIO','SUBADQUIRENTE','EMPRESAS','VAREJO', 'ISO']).fillna(0)

        df_cart_sts_aging_qtd_porc['TOTALMR'] = df_cart_sts_aging_qtd_porc.sum(axis = 1)
        df_cart_sts_aging_qtd_porc.loc['TOTALMR'] = df_cart_sts_aging_qtd_porc.sum(axis = 0)

        lista_array = list(df_cart_sts_aging_qtd_porc.to_records(index=True, column_dtypes=dict))
        
        query_insert = "INSERT /*array */ INTO credito.cart_cobr_sts_cart_aging_porc (AGING, ZERO, GERENTEVIRTUAL, LONGTAIL, PARCERIAS, BERCARIO,SUBADQUIRENTE,EMPRESAS,VAREJO, ISO, TOTALMR) values (:0, :1, :2, :3, :4, :5, :6, :7, :8, :9, :10)"
        
        table_name = ("credito.cart_cobr_sts_cart_aging_porc")
        insertdb.insertDataBase(lista_array, query_insert, table_name)
        list.clear(lista_array)

    except Exception as err:
        log.logger.error(err)
        raise err
#print(datetime.date_time_zn().now - startTime)

    conn.close()