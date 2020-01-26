import pandas as pd
import numpy as np
import datetime


#数据下载的格式必须是英文
def open_csv(start_date, end_date):
    try:
        reguser = pd.read_csv(r'C:\Users\Jesse\Downloads\member_analysis_report_New Registrations_' + start_date + '-' + end_date + '.csv', encoding='utf-8-sig')
        return reguser
    except:
        print('File Not Found Error')


#清洗所有NaN的栏位
def fillna(reguser):
    reguser['Total Deposit Amount'] = reguser.pop('Total Deposit Amount').fillna(0.00)
    reguser['Total Withdrawal Amount'] = reguser.pop('Total Withdrawal Amount').fillna(0.00)
    reguser['Total Adjustment Amount'] = reguser.pop('Total Adjustment Amount').fillna(0.00)
    reguser.insert(2, 'Source Name', reguser.pop('Source Name').fillna(''))
    reguser.insert(8, 'First Deposit Date', reguser.pop('First Deposit Date').fillna(''))
    return reguser


#日期栏位只保留 YYYY/MM/DD
def cleasing_table(reguser):
    x = reguser['Regisration Date']
    new_RegDate = []
    for i in x:
        Redate = i.split(' ')[0].split('/')
        newRedate = '%s-%s-%s' % (Redate[2], Redate[0], Redate[1])
        new_RegDate.append(newRedate)
    DateSer = pd.Series(new_RegDate)
    reguser['Regisration Date'] = DateSer
    y = reguser['First Deposit Date']
    new_FDDate = []
    for i in y:
        date = i.split(' ')[0].split('/')
        if len(date) == 1:
            new_FDDate.append(date[0])
        else:
            newdate = '%s-%s-%s' % (date[2], date[0], date[1])
            new_FDDate.append(newdate)
    FdSer = pd.Series(new_FDDate)
    reguser['First Deposit Date'] = FdSer 
    return reguser


#输出csv
def exoprt_csv(reguser, start_date, end_date ):
    try:
        reguser.to_csv(r'C:\Users\Jesse\Downloads\member_analysis_report_New Registrations_' + start_date + '-' + end_date + '.csv', encoding='utf-8-sig', index=False)
        print('Csv_file export')
    except:
        print('File export failed')
    
        
if __name__ == '__main__':
    start_date = input('Date Import（Format YYYY_MM_DD）:')
    end_date = input('Date Import（Format YYYY_MM_DD）:')
    reguser = open_csv(start_date, end_date)
    fillna_table = fillna(reguser)
    new_table = cleasing_table(fillna_table)
    exoprt_csv(new_table, start_date, end_date)
    