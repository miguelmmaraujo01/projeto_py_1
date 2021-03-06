import connect as conexao
import sys
#import os
import log

#conn = conexao.connect_db().oracle  
#cursor = conn.cursor() 

def drop_create_tb_cart():
    conn = conexao.connect_db().oracle
    log.logger.info(conn)
    cursor = conn.cursor() 
    try:
        try:
            cursor.execute('drop index credito.idx_tmpdbcredpf_01')
        except Exception as err:
            log.logger.info('index idx_tmpdbcredpf_01 not exists')#print('idx tmp_dbm_credito_pf nao existe')
        pass

        try:
            cursor.execute('drop table credito.tmp_dbm_credito_pf')
        except Exception as err:
            log.logger.info('table credito.tmp_dbm_credito_pf not exists')#print('idx tmp_dbm_credito_pf nao existe')
        pass

        cursor.execute(""" create table credito.tmp_dbm_credito_pf as select documento as originador, anomes, translate( upper(carteira),'ÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜáçéíóúàèìòùâêîôûãõëü','ACEIOUAEIOUAEIOUAOEUaceiouaeiouaeiouaoeu') as carteira from credito.dbm_credito_massa where carteira is not null""")
        cursor.execute('create index idx_tmpdbcredpf_01 on credito.tmp_dbm_credito_pf (originador) tablespace tsicredito')
    
        log.logger.info('object credito.tmp_dbm_credito_pf - idx_tmpdbcredpf_01 created')#print('tabela tmp_dbm_credito_pf criada')
        
        try:
            cursor.execute('drop index credito.idx_tmpdbcredpj_01')
        except Exception as err:
            log.logger.info('index idx_tmpdbcredpj_01 not exists')#print('idx tmp_dbm_credito_pf nao existe')
        pass
            
        try:
            cursor.execute('drop table credito.tmp_dbm_credito_pj')
        except Exception as err:
           log.logger.info('table credito.tmp_dbm_credito_pj not exists')# print('tabela tmp_dbm_credito_pj nao existe')
        pass    

        cursor.execute("""create table credito.tmp_dbm_credito_pj as select documento as originador, anomes, translate(upper(carteira),'ÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜáçéíóúàèìòùâêîôûãõëü','ACEIOUAEIOUAEIOUAOEUaceiouaeiouaeiouaoeu') as carteira from credito.dbm_credito_massa_pj where carteira is not null""")
        cursor.execute('create index idx_tmpdbcredpj_01 on credito.tmp_dbm_credito_pj (originador) tablespace tsicredito')
        
        log.logger.info('object credito.tmp_dbm_credito_pj - idx_tmpdbcredpj_01 created')#print('tabela tmp_dbm_credito_pf criada')
        #os.system("pause")

    except Exception as err:
        log.logger.error(err)
        #exit
        #print('Erro criacao das tabelas tmp pf pj')
        #sys.exit()
        #os.system("pause")


def query_select():

    sql = ("""select 
            pf.idt_operacao, pf.num_cpf_socio as originador , pf.anomes, nvl(pf.carteira,0) as carteira, pf.num_atraso_total_dias, pf.IND_DIVISAO, pf.num_saldo_contabil
            ,max(pf.row_num)
            from (
            select a.idt_operacao, a.num_cpf_socio, a.anomes, nvl(b.carteira,0) as carteira, a.num_atraso_total_dias,a.des_forma_pagamento as IND_DIVISAO, a.num_saldo_contabil
            ,a.num_rank
            ,ROW_NUMBER() OVER (PARTITION BY a.num_rank ORDER BY a.num_rank DESC ) AS ROW_NUM  
                            from credito.cart_cobr_arq_delayed_rec a
                            left join credito.tmp_dbm_credito_pf b on (a.num_cpf_socio = b.originador) and (a.anomes = b.anomes)
                            where trunc(a.dat_import) >= trunc(sysdate)
                            and a.ind_tp_tomador  = 'pf' 
                            )pf
                            group by pf.idt_operacao, pf.num_cpf_socio, pf.anomes, pf.carteira, pf.num_atraso_total_dias,pf.IND_DIVISAO, pf.num_saldo_contabil
            union all
            select 
            pf.idt_operacao, pf.num_cnpj as originador, pf.anomes, nvl(pf.carteira,0) as carteira, pf.num_atraso_total_dias, pf.IND_DIVISAO, pf.num_saldo_contabil
            ,max(pf.row_num)
            from (
            select a.idt_operacao, a.num_cnpj, a.anomes, nvl(b.carteira,0) as carteira, a.num_atraso_total_dias,a.des_forma_pagamento as IND_DIVISAO, a.num_saldo_contabil
            ,a.num_rank
            ,ROW_NUMBER() OVER (PARTITION BY a.num_rank ORDER BY a.num_rank DESC ) AS ROW_NUM  
                            from credito.cart_cobr_arq_delayed_rec a
                            left join credito.tmp_dbm_credito_pj b on (a.num_cnpj = b.originador) and (a.anomes = b.anomes)
                            where trunc(a.dat_import) >= trunc(sysdate)
                            and a.ind_tp_tomador  = 'pj' 
                            )pf
                            group by pf.idt_operacao, pf.num_cnpj, pf.anomes, pf.carteira, pf.num_atraso_total_dias,pf.IND_DIVISAO, pf.num_saldo_contabil    
        """)


    return sql

sql = query_select()




'''


    sql = ("""select a.idt_operacao, a.num_cpf_socio, a.anomes, nvl(b.carteira,0) as carteira, a.num_atraso_total_dias,a.des_forma_pagamento as IND_DIVISAO, a.num_saldo_contabil
                from credito.cart_cobr_arq_delayed_rec a
                left join credito.tmp_dbm_credito_pf b on (a.num_cpf_socio = b.originador) and (a.anomes = b.anomes)
                where trunc(a.dat_import) >= trunc(sysdate)
                and a.ind_tp_tomador  = 'pf' --'in ('pessoa física', 'pf')
                union all
            select a.idt_operacao, a.num_cnpj, a.anomes, nvl(b.carteira,0) as carteira, a.num_atraso_total_dias,a.des_forma_pagamento as IND_DIVISAO, a.num_saldo_contabil
                from credito.cart_cobr_arq_delayed_rec a
                left join credito.tmp_dbm_credito_pj b on (a.num_cnpj = b.originador) and (a.anomes = b.anomes)
                where trunc(a.dat_import) >= trunc(sysdate)
                and a.ind_tp_tomador = 'pj' --in ('pessoa jurídica', 'pj')
                """)


#query considerando max rownumber da ultima exec do dia

select 
pf.idt_operacao, pf.num_cpf_socio as originador , pf.anomes, nvl(pf.carteira,0) as carteira, pf.num_atraso_total_dias, pf.IND_DIVISAO, pf.num_saldo_contabil
,max(pf.row_num)
from (
select a.idt_operacao, a.num_cpf_socio, a.anomes, nvl(b.carteira,0) as carteira, a.num_atraso_total_dias,a.des_forma_pagamento as IND_DIVISAO, a.num_saldo_contabil
,rank_num
,ROW_NUMBER() OVER (PARTITION BY RANK_NUM ORDER BY RANK_NUM DESC ) AS ROW_NUM  
                from credito.teste_rank_py a
                left join credito.tmp_dbm_credito_pf b on (a.num_cpf_socio = b.originador) and (a.anomes = b.anomes)
                where trunc(a.dat_import) >= trunc(sysdate)
                and a.ind_tp_tomador  = 'pf' 
                )pf
                group by pf.idt_operacao, pf.num_cpf_socio, pf.anomes, pf.carteira, pf.num_atraso_total_dias,pf.IND_DIVISAO, pf.num_saldo_contabil
union all
select 
pf.idt_operacao, pf.num_cnpj as originador, pf.anomes, nvl(pf.carteira,0) as carteira, pf.num_atraso_total_dias, pf.IND_DIVISAO, pf.num_saldo_contabil
,max(pf.row_num)
from (
select a.idt_operacao, a.num_cnpj, a.anomes, nvl(b.carteira,0) as carteira, a.num_atraso_total_dias,a.des_forma_pagamento as IND_DIVISAO, a.num_saldo_contabil
,rank_num
,ROW_NUMBER() OVER (PARTITION BY RANK_NUM ORDER BY RANK_NUM DESC ) AS ROW_NUM  
                from credito.teste_rank_py a
                left join credito.tmp_dbm_credito_pj b on (a.num_cnpj = b.originador) and (a.anomes = b.anomes)
                where trunc(a.dat_import) >= trunc(sysdate)
                and a.ind_tp_tomador  = 'pj' 
                )pf
                group by pf.idt_operacao, pf.num_cnpj, pf.anomes, pf.carteira, pf.num_atraso_total_dias,pf.IND_DIVISAO, pf.num_saldo_contabil




'''
