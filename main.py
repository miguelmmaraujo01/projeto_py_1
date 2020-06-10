import log
import pag_loader_daleyd_arq as etapa1
import pag_carteira_aging as etapa2
import pag_status_carteira as etapa3

import p2p_pag_loader_daleyd_arq as p2p_etapa1
import p2p_pag_carteira_aging as p2p_etapa2

# endponint receber o nome do arq ou chave que o s3 gera par ao arquivo, arquivo deve ter nomes dif
identificador_arquivo = 'delayed-receivables'
#identificador_arquivo = 'biva'

def ordem_exec(param_ind_arquivo):
    try:
        if (param_ind_arquivo.count("biva")) or (param_ind_arquivo.count("p2p")):
            print(param_ind_arquivo)
            p2p_etapa1.p2p_pag_loader_delayd_receivables_arq(identificador_arquivo)
            p2p_etapa2.p2p_create_tb_carteira_aging(identificador_arquivo)

        elif (param_ind_arquivo.count("delayed")): #'delayed-receivables'
            print(param_ind_arquivo)
            #etapa1.pag_loader_delayd_receivables_arq(identificador_arquivo)
            #etapa2.create_tb_carteira_aging(identificador_arquivo)
            etapa3.create_tb_carteira_status()
        else:
            log.logger.error('Name param invalid')

    except Exception as err:
        log.logger.error(err)
        raise err


ordem_exec(identificador_arquivo)



