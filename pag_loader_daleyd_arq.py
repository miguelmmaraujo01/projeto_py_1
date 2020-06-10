import pandas as pd
#import os
import sys
from io import StringIO
import connect as conexao
import datetimezone as datetime
import insertdb
import listcases
import read_data_s3 as reads3
import log

#startTime = datetime.date_time_zn().now

def pag_loader_delayd_receivables_arq(identificador_arquivo):
	df = pd.read_csv(reads3.busca_arquivo_s3(identificador_arquivo), sep=",", doublequote=False)
	try:
		arq_csv_colun_sele = df[['Id','Contrato','Cnpj','Cpf socio','Situação','Tipo de tomador','Entidade','Data de contrato','Taxa efetiva (a.m.)','Forma de pagamento','Saldo Devedor Atual','Saldo Contábil','Valor em atraso','Atraso total (dias)']].fillna('0')
		#print(arq_csv_colun_sele.head())
		#os.system("pause")
		arq_csv_colun_sele = arq_csv_colun_sele.rename(columns={"Id": "idt_operacao"
			,"Contrato": "num_contrato"
			,"Cnpj" : "num_cnpj"
			,"Cpf socio" : "num_cpf_socio"
			,"Situação": "des_situacao"
			,"Tipo de tomador": "ind_tp_tomador"
			,"Entidade": "nam_entidade"
			,"Data de contrato": "dat_contrato"
			,"Taxa efetiva (a.m.)": "num_taxa_efetiva"
			,"Saldo Devedor Atual": "num_saldo_devedor_atual"
			,"Saldo Contábil": 'num_saldo_contabil'
			,"Forma de pagamento": "des_forma_pagamento"
			,"Valor em atraso": "num_valor_atraso"
			,"Atraso total (dias)": "num_atraso_total_dias"
			})

		
		arq_csv_colun_sele['num_cpf_socio'] = arq_csv_colun_sele['num_cpf_socio'].astype(str)
		arq_csv_colun_sele['num_cpf_socio'] = arq_csv_colun_sele.num_cpf_socio.str.pad(11,side='left',fillchar='0')

		arq_csv_colun_sele['num_cnpj'] = arq_csv_colun_sele['num_cnpj'].astype(str)
		arq_csv_colun_sele['num_cnpj'] = arq_csv_colun_sele.num_cnpj.str.pad(14,side='left',fillchar='0')

		arq_csv_colun_sele['num_saldo_devedor_atual'] = arq_csv_colun_sele['num_saldo_devedor_atual'].str.replace('.', '').str.replace('$', '').str.replace('R', '').fillna(0)
		#para pag usa saldocontabil para p2p usa saldodevedor
		arq_csv_colun_sele['num_saldo_contabil'] = arq_csv_colun_sele['num_saldo_contabil'].str.replace('.', '').str.replace('$', '').str.replace('R', '').fillna(0)
		arq_csv_colun_sele['num_valor_atraso'] = arq_csv_colun_sele['num_valor_atraso'].str.replace('.', '').str.replace('$', '').str.replace('R', '').fillna(0)

		arq_csv_colun_sele['des_forma_pagamento'] = listcases.lista_forma_pagamento(arq_csv_colun_sele['des_forma_pagamento'])

		arq_csv_colun_sele['ind_tp_tomador'] = listcases.lista_tp_tomador(arq_csv_colun_sele['ind_tp_tomador'])

		arq_csv_colun_sele['anomes'] = arq_csv_colun_sele['dat_contrato'].str[0:7]
		arq_csv_colun_sele['num_taxa_efetiva'] = arq_csv_colun_sele['num_taxa_efetiva'].fillna(0)*100 
		arq_csv_colun_sele['num_atraso_total_dias'] = arq_csv_colun_sele['num_atraso_total_dias'].astype('int64').fillna(0)

		#print(arq_csv_colun_sele.head())
		#os.system("pause")

		lista_array = list(arq_csv_colun_sele.to_records(index=False, column_dtypes=dict))
		log.logger.info('0 - credito.cart_cobr_arq_delayed_rec')
		
		query_insert =("INSERT /*array */ INTO credito.cart_cobr_arq_delayed_rec (idt_operacao ,num_contrato ,num_cnpj ,num_cpf_socio ,des_situacao ,ind_tp_tomador ,nam_entidade ,dat_contrato, num_taxa_efetiva, des_forma_pagamento ,num_saldo_devedor_atual, num_saldo_contabil ,num_valor_atraso ,num_atraso_total_dias, anomes) values (:1, :2, :3, :4, :5, :6, :7 ,to_Date(:8,'yyyy-mm-dd'), :9, :10, :11, :12, :13, :14, :15)")
		table_name = ("credito.cart_cobr_arq_delayed_rec")
		insertdb.insertDataBase(lista_array, query_insert, table_name)
	except Exception as err:
		log.logger.error(err)
		raise err
#print( datetime.date_time_zn().now - startTime)
