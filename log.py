import logging
#from sys import stdout
#import pandas as pd
#from datetime import date
import datetimezone as datetime

#current_date = str(date.today())

#current_date = str(datetime.date_time_zn().now)

def config_log():
    logging.basicConfig(
        #filename='./logs/log_%s.log' % current_date,
        level=logging.INFO,
        format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
        filemode= 'w')
    logger = logging.getLogger()
    
    return logger

logger = config_log()