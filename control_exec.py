import connect as conexao
import datetimezone as datetime
import log
import pandas as pd

def control_insert_exec(param_ind_job, param_nam_job, param_nam_etapa, param_nam_arq, param_retorno, param_descr_retorno, param_nam_card, param_nam_area):

    #param_dat_inicio = datetime.date_time_zn().dtt
    conn = conexao.connect_database().oracle
    
    try:
        #idt_seq, idt_job, nam_job, nam_etapa, nam_arq, dat_inicio, dat_fim, ind_exec, desc_error
        param_dat_fim = datetime.date_time_zn().dtt
        cursor = conn.cursor()
        
        param_seq = pd.read_sql_query('select credito.seq_cart_cobr_log_02.nextval as idt_seq from dual', conn)#cursor.prepare('credito.seq_cart_cobr_01.nextval')
        param_seq = (str(param_seq['IDT_SEQ'][0]))
        
        cursor.prepare("""insert into credito.control_exec_akron_log (idt_seq, idt_job, nam_job, nam_etapa, nam_arq, ind_exec, desc_error, nam_card_jira, nam_area, dat_inicio, dat_fim) values (:1,:2, :3, :4, :5, :6, :7, :8, :9, to_date(:10,'yyyy-mm-dd hh24:mi:ss'), to_date(:11,'yyyy-mm-dd hh24:mi:ss'))""")
        lista = [(param_seq, param_ind_job, param_nam_job, param_nam_etapa, param_nam_arq, param_retorno, param_descr_retorno, param_nam_card, param_nam_area, param_dat_fim, param_dat_fim )]
        print(lista)
        cursor.executemany(None, lista)

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as err:
        log.logger.error(err)    
        
def control_update_exec_ini(param_ind_job, param_nam_job, param_nam_etapa):

    param_dat_ini = datetime.date_time_zn().dtt
    conn = conexao.connect_database().oracle

    try:
        cursor = conn.cursor()
        query = """update credito.control_exec_akron_job 
                    set dat_inicio = to_date(:1,'yyyy-mm-dd hh24:mi:ss')
                    ,dat_fim = NULL
                    ,ind_exec = NULL
                    ,desc_error = NULL  
                    where idt_job = :2 and nam_job =:3 and nam_etapa = :4       
                """
        cursor.execute(query, (param_dat_ini,param_ind_job, param_nam_job , param_nam_etapa))

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as err:
        log.logger.error(err)    

def control_update_exec_fim(param_ind_job, param_nam_etapa, param_retorno, param_descr_retorno):

    param_dat_fim = datetime.date_time_zn().dtt
    conn = conexao.connect_database().oracle

    try:
        cursor = conn.cursor()
        query = """update credito.control_exec_akron_job set dat_fim = to_date(:1,'yyyy-mm-dd hh24:mi:ss'), ind_exec = :2, desc_error = :3 where idt_job = :4 and dat_inicio in (select (dat_inicio) from credito.control_exec_akron_job where idt_job = :5 and nam_etapa = :6 and dat_fim is null)"""
        cursor.execute(query, (param_dat_fim, param_retorno,param_descr_retorno ,param_ind_job, param_ind_job, param_nam_etapa))

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as err:
        log.logger.error(err)    
