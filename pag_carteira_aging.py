import pandas as pd
#import sys
#from io import StringIO
#import os # usa para pause  os.system("pause")
#import connect as conexao
#import datetimezone as datetime
import listcases
import insertdb
import read_data_s3 as reads3
import log

#log.logger.info('pag_carteira_aging.py')
def create_tb_carteira_aging(identificador_arquivo):
    
    df = pd.read_csv(reads3.busca_arquivo_s3(identificador_arquivo), sep=",", doublequote=False)
    #arq_csv_colun_sele = df[['Id','Contrato','Situação','Tipo de tomador','Entidade','Data de contrato','Saldo Devedor Atual','Saldo Contábil','Valor em atraso','Forma de pagamento','Atraso total (dias)']].fillna(0)
    try:
        arq_csv_colun_sele = df[['Id','Situação','Data de contrato','Saldo Contábil','Valor em atraso','Forma de pagamento','Atraso total (dias)']].fillna(0)

        arq_csv_colun_sele = arq_csv_colun_sele.rename(columns={"Id": "idt_operacao"
            #,"Contrato": "num_contrato"
            #,"Cnpj" : "num_cnpj"
            #,"Cpf socio" : "num_cpf_socio" #"{0:011d}".format(1234567)
            #,"Situação": "des_situacao"
            #,"Tipo de tomador": "ind_tp_tomador"
            #,"Entidade": "nam_entidade"
            ,"Data de contrato": "dat_contrato"
            #,"Saldo Devedor Atual": "num_saldo_devedor_atual"
            ,"Saldo Contábil": "num_saldo_contabil"
            ,"Forma de pagamento": "des_forma_pagamento"
            ,"Valor em atraso": "num_valor_atraso"
            ,"Atraso total (dias)": "num_atraso_total_dias"})

        #para pag usa saldocontabil para p2p usa saldodevedor
        arq_csv_colun_sele['num_saldo_contabil'] = arq_csv_colun_sele['num_saldo_contabil'].str.replace('.', '').str.replace(',', '.').str.replace('$', '').str.replace('R', '').astype('float64')
        arq_csv_colun_sele['ind_divisao'] = listcases.lista_forma_pagamento(arq_csv_colun_sele['des_forma_pagamento'])

        df_agrupado_cart_cobr = pd.DataFrame(arq_csv_colun_sele, columns = ['ind_divisao', 'num_atraso_total_dias', 'num_saldo_contabil'])

        log.logger.info('1 - credito.cart_cobr_sum')#print('1 - credito.cart_cobr_sum')
        #gera a primeira tabela consolidado valores somatoria e porc
        df_agrupado_group_sum = arq_csv_colun_sele.groupby(['ind_divisao'])['num_saldo_contabil'].sum().to_frame() 
        df_agrupado_group_sum['porcent'] = df_agrupado_group_sum['num_saldo_contabil'] / df_agrupado_group_sum['num_saldo_contabil'].sum()*100 
        df_agrupado_group_sum.loc['totalmr'] = df_agrupado_group_sum.sum(axis = 0)

        lista_array = list(df_agrupado_group_sum.to_records(index=True, column_dtypes=dict))

        query_insert = "INSERT /*array */ INTO credito.cart_cobr_sum (cart_cobr, totalmr, porc) values (:1, :2, :3)"
        table_name = ("credito.cart_cobr_sum")
        insertdb.insertDataBase(lista_array, query_insert, table_name)

        log.logger.info('2 - credito.cart_cobr_aging_valor')#print('2 - credito.cart_cobr_aging_valor')
        #gera a seg tabela consolidado valores somatoria com aging dias
        df_agrupado_cart_cobr = pd.DataFrame(arq_csv_colun_sele, columns = ['idt_operacao','ind_divisao', 'num_atraso_total_dias', 'num_saldo_contabil'])  

        df_agrupado_cart_cobr['aging'] = listcases.lista_atraso_dias(df_agrupado_cart_cobr['num_atraso_total_dias']) 

        df_cart_aging_valor = df_agrupado_cart_cobr.groupby(['aging', 'ind_divisao'])['num_saldo_contabil'].sum().to_frame().reset_index()
        df_cart_aging_valor = df_cart_aging_valor.pivot_table(values='num_saldo_contabil', index='aging', columns='ind_divisao',aggfunc=sum).fillna(0)

        df_cart_aging_valor = pd.DataFrame(df_cart_aging_valor, columns = ['Capital', 'WhiteLabel', 'totalmr']).fillna(0) 

        df_cart_aging_valor['totalmr'] = df_cart_aging_valor.sum(axis = 1)
        df_cart_aging_valor.loc['totalmr'] = df_cart_aging_valor.sum(axis = 0)

        lista_array = list(df_cart_aging_valor.to_records(index=True, column_dtypes=dict))

        query_insert = "INSERT /*array */ INTO credito.cart_cobr_aging_valor (aging, capital, whitelabel, totalmr) values (:1, :2, :3, :4)"
        table_name = ("credito.cart_cobr_aging_valor")
        insertdb.insertDataBase(lista_array, query_insert, table_name)

        log.logger.info('3 - credito.cart_cobr_aging_qtd')#print('3 - credito.cart_cobr_aging_qtd')
        #gera a terc tabela consolidado valores qtd ope com aging dias

        df_cart_aging_qtd_op = df_agrupado_cart_cobr.groupby(['aging', 'ind_divisao'])['idt_operacao'].size().to_frame().reset_index()
        df_cart_aging_qtd_op = df_cart_aging_qtd_op.pivot_table(values='idt_operacao', index=['aging'], columns='ind_divisao',aggfunc=pd.Series.unique).fillna(0)

        df_cart_aging_qtd_op = pd.DataFrame(df_cart_aging_qtd_op, columns = ['Capital', 'WhiteLabel', 'totalmr']).fillna(0) 

        df_cart_aging_qtd_op['totalmr'] = df_cart_aging_qtd_op.sum(axis = 1)
        df_cart_aging_qtd_op.loc['totalmr'] = df_cart_aging_qtd_op.sum(axis = 0)

        lista_array = list(df_cart_aging_qtd_op.to_records(index=True, column_dtypes=dict))
        query_insert = "INSERT /*array */ INTO credito.cart_cobr_aging_qtd (aging, capital, whitelabel, totalmr) values (:1, :2, :3, :4)"
        table_name = ("credito.cart_cobr_aging_qtd")
        insertdb.insertDataBase(lista_array, query_insert, table_name)

        log.logger.info('4 - credito.cart_cobr_aging_porc')#print('4 - credito.cart_cobr_aging_porc')
        #gera a quart tabela consolidado porc  aging dias

        df_cart_aging_qtd_porc = pd.DataFrame(df_cart_aging_qtd_op, columns = ['Capital','WhiteLabel'])  
        df_cart_aging_qtd_porc = df_cart_aging_qtd_porc.drop('totalmr')

        df_cart_aging_qtd_porc['Capital'] = df_cart_aging_qtd_porc['Capital']/df_cart_aging_qtd_porc['Capital'].sum(axis = 0)*100 
        df_cart_aging_qtd_porc['WhiteLabel'] = df_cart_aging_qtd_porc['WhiteLabel']/df_cart_aging_qtd_porc['WhiteLabel'].sum(axis = 0)*100 

        df_cart_aging_qtd_op = pd.DataFrame(df_cart_aging_qtd_op, columns = ['Capital', 'WhiteLabel', 'totalmr']).fillna(0) 

        df_cart_aging_qtd_porc['totalmr'] = df_cart_aging_qtd_porc.sum(axis = 1)
        df_cart_aging_qtd_porc.loc['totalmr'] = df_cart_aging_qtd_porc.sum(axis = 0)

        lista_array = list(df_cart_aging_qtd_porc.to_records(index=True, column_dtypes=dict))

        query_insert = "INSERT /*array */ INTO credito.cart_cobr_aging_porc (aging, capital, whitelabel, totalmr) values (:1, :2, :3, :4)"
        table_name = ("credito.cart_cobr_aging_porc")
        insertdb.insertDataBase(lista_array, query_insert, table_name)

    except Exception as err:
        log.logger.error(err)
        raise err
