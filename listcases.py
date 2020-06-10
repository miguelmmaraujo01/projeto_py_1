#import pandas as pd

def lista_forma_pagamento(param_des_forma_pagamento):
    list_column = []
    for coluna_nova in param_des_forma_pagamento:
        if coluna_nova == 'Boletos':
            col_n = 'WhiteLabel'
        elif coluna_nova == 'Faturamento':
            col_n = 'Capital'
        else:
            col_n = 'na'
        list_column.append(col_n)   
    return list_column

def lista_tp_tomador(param_ind_tp_tomador):
    list_column = []
    for coluna_nova in param_ind_tp_tomador:
        if coluna_nova == 'pessoa física':
            col_n = 'pf'
        elif coluna_nova == 'pessoa jurídica':
            col_n = 'pj'
        else:
            col_n = 'na'
        list_column.append(col_n)
    return list_column

def lista_atraso_dias(param_num_atraso_total_dias):
    list_column = []
    for coluna_nova in param_num_atraso_total_dias:
        if coluna_nova >= 0 and coluna_nova <=5:
            col_n = '(01) a vence'
        elif coluna_nova > 5 and coluna_nova <=15:
            col_n = '(02) 06 - 15'
        elif coluna_nova > 15 and coluna_nova <= 30:
            col_n = '(03) 16 - 30'
        elif coluna_nova > 30 and coluna_nova <= 60:
            col_n = '(04) 31 - 60'
        elif coluna_nova > 60 and coluna_nova <= 90:
            col_n = '(05) 61 - 90'
        elif coluna_nova > 90 and coluna_nova <= 120:
            col_n = '(06) 91 - 120'    
        elif coluna_nova > 120 and coluna_nova <= 150:
            col_n = '(07) 121 - 150'    
        elif coluna_nova > 150 and coluna_nova <= 180:
            col_n = '(08) 151 - 180'
        elif coluna_nova > 180 and coluna_nova <= 360:
            col_n = '(09) 181 - 360'        
        elif coluna_nova > 360:
            col_n = '(10) 361'
        list_column.append(col_n)
    return list_column

def p2p_ind_divisao(param_dat, param_sit):
    list_column = []
    for coluna_dat, coluna_sit in zip(param_dat, param_sit):
        if coluna_dat > '30/09/2017' and coluna_sit != 'recomprada': 
            col_n = 'pagseguro_p2p'
        elif coluna_dat <= '30/09/2017' and coluna_sit != 'recomprada':
            col_n = 'biva_p2p'
        elif coluna_sit == 'recomprada':
            col_n = 'recompra'
        else:
            col_n = 'other'
        list_column.append(col_n)
    return list_column