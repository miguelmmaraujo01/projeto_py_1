import connect as conexao
import datetimezone as datetime
import log
import pandas as pd

def control_insert_exec(param_ind_job, param_nam_job, param_nam_etapa, param_nam_arq):

    #param_dat_inicio = datetime.date_time_zn().dtt
    conn = conexao.connect_database().oracle
    
    try:
        #idt_seq, idt_job, nam_job, nam_etapa, nam_arq, dat_inicio, dat_fim, ind_exec, desc_error
        cursor = conn.cursor()
        
        param_seq = pd.read_sql_query('select credito.seq_cart_cobr_01.nextval as idt_seq from dual', conn)#cursor.prepare('credito.seq_cart_cobr_01.nextval')
        param_seq = (str(param_seq['IDT_SEQ'][0]))
        
        cursor.prepare('insert into credito.control_exec_akron_job (idt_seq, idt_job, nam_job, nam_etapa, nam_arq) values (:1,:2, :3, :4, :5)')
        lista = [(param_seq, param_ind_job, param_nam_job, param_nam_etapa, param_nam_arq)]

        cursor.executemany(None, lista)

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as err:
        log.logger.error(err)    


def control_update_exec(param_ind_job, param_nam_etapa, param_retorno, param_descr_retorno):

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
