import log
import pag_loader_daleyd_arq as etapa1
import pag_carteira_aging as etapa2
import pag_status_carteira as etapa3

import p2p_pag_loader_daleyd_arq as p2p_etapa1
import p2p_pag_carteira_aging as p2p_etapa2
import control_exec as ctrl

# endponint receber o nome do arq ou chave que o s3 gera par ao arquivo, arquivo deve ter nomes dif
identificador_arquivo = 'p2p-delayed-receivables'
#identificador_arquivo = 'biva'

def ordem_exec(param_ind_arquivo):
    try:
        if (param_ind_arquivo.count("biva")) or (param_ind_arquivo.count("p2p")):    
            print(param_ind_arquivo)
            #
            ind_job = '4'
            nam_job = 'p2p_pag_loader_daleyd_arq'
            nam_etapa = 'p2p_etapa1'
            nam_arq = '' 

            ctrl.control_insert_exec(ind_job,nam_job,nam_etapa,nam_arq)           
            retorno = p2p_etapa1.p2p_pag_loader_delayd_receivables_arq(identificador_arquivo)
            ctrl.control_update_exec(ind_job, nam_etapa, retorno[0], retorno[1])

            if retorno[0] != 0:
                raise SystemExit
            else:

                #
                ind_job = '5'
                nam_job = 'p2p_pag_carteira_aging'
                nam_etapa = 'p2p_etapa2'
                nam_arq = '' 

                ctrl.control_insert_exec(ind_job,nam_job,nam_etapa,nam_arq)               
                retorno = p2p_etapa2.p2p_create_tb_carteira_aging(identificador_arquivo)
                ctrl.control_update_exec(ind_job, nam_etapa, retorno[0], retorno[1])
        
        elif  (param_ind_arquivo.count("delayed")): #'delayed-receivables' 
            #
            ind_job = '1'
            nam_job = 'pag_loader_daleyd_arq'
            nam_etapa = 'etapa1'
            nam_arq = '' 

            ctrl.control_insert_exec(ind_job,nam_job,nam_etapa,nam_arq)
            retorno = etapa1.pag_loader_delayd_receivables_arq(identificador_arquivo)
            ctrl.control_update_exec(ind_job, nam_etapa, retorno[0], retorno[1])

            if retorno[0] != 0:
                raise SystemExit
            else:

                ##
                ind_job = '2'
                nam_job = 'pag_carteira_aging'
                nam_etapa = 'etapa2'
                nam_arq = ''   

                ctrl.control_insert_exec(ind_job,nam_job,nam_etapa,nam_arq)
                retorno = etapa2.create_tb_carteira_aging(identificador_arquivo)
                ctrl.control_update_exec(ind_job, nam_etapa, retorno[0], retorno[1])

                
                ###
                ind_job = '3'
                nam_job = 'pag_status_carteira'
                nam_etapa = 'etapa3'
                nam_arq = '' 

                ctrl.control_insert_exec(ind_job,nam_job,nam_etapa,nam_arq)
                retorno = etapa3.create_tb_carteira_status()
                ctrl.control_update_exec(ind_job, nam_etapa, retorno[0], retorno[1])

        else:
            log.logger.error('Name param invalid')

    except Exception as err:
        log.logger.error(err)
        raise err


ordem_exec(identificador_arquivo)
