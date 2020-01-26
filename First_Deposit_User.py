import pandas as pd 
import numpy as np 
import datetime 


#csv file must be downloaded in English version else the file expot will be failed  
def open_csv(start_date, end_date):
    try:
        df = pd.read_csv(r'C:\Users\Jesse\Downloads\member_analysis_report_First Deposit Users_' + start_date + '-' + end_date + '.csv', encoding='utf-8-sig')
        return df 
    except:
        print('File Not Found Error')


def cleansing_table(df):
    df['Source Name'] = df['Source Name'].fillna('')
    Dates = df['First Deposit Date']
    Date = []
    for i in Dates: #restructed the date 
        new_date = i.split(' ')[0].split('/')
        the_date = '%s-%s-%s' % (new_date[2], new_date[0], new_date[1])
        Date.append(the_date)
    DateSer = pd.Series(Date)
    df['First Deposit Date'] = DateSer
    return df


def export_csv(df, start_date, end_date):
    try:
        df.to_csv(r'C:\Users\Jesse\Downloads\member_analysis_report_First Deposit Users_' + start_date + '-' + end_date + '.csv', encoding='utf-8-sig', index=False)
        print('Csv_File exort')
    except:
        print('File export failed')

    
if __name__ == '__main__':
    start_date = input('Date Import（Format YYYY_MM_DD）:')
    end_date = input('Date Import（Format YYYY_MM_DD）:')
    df = open_csv(start_date, end_date)
    df = cleansing_table(df)
    export_csv(df, start_date, end_date)