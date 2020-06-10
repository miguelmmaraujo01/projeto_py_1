import datetime
import pytz

class date_time_zn():
    def __init__(self):
        current_date = datetime.datetime.now(pytz.timezone('America/Sao_Paulo'))
        self.hour = (current_date + datetime.timedelta(hours=0)).strftime('%H')
        self.day = str(current_date.strftime('%d'))
        self.month = str(current_date.strftime('%m'))
        self.year = current_date.strftime('%Y')
        self.tt = str(current_date.strftime('%H%M%S%f'))
        self.data = str(current_date.strftime('%Y%m%d'))
        self.dtt = str(current_date.strftime('%Y-%m-%d %H:%M:%S'))
        self.now = datetime.datetime.now()

def var_datetime():
    return date_time_zn()
