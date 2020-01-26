import pandas as pd
import numpy as np 
import datetime


def open_csv(csv_date):
    try:
        df = pd.read_csv(r'C:\Users\Jesse\Downloads\member_analysis_report_Deposit Counts_' + csv_date + '.csv')
        return df
    except:
        print('Read file error')


def cleansing_table(df, day):
    df.insert(0, 'Date', day)
    return df


if __name__ == '__main__':
    begin = input('Input the begin date(Format:YYYY_MM_DD)')
    end = input('Input the end date(Format:YYYY_MM_DD):')
    begin = datetime.date(int(begin.split('_')[0]),int(begin.split('_')[1]),int(begin.split('_')[2]))
    end = datetime.date(int(end.split('_')[0]),int(end.split('_')[1]),int(end.split('_')[2]))
    df = pd.DataFrame(columns = ['Date', 'User Name', 'Source', 'Source Name', 'Vip Level', 'Risk Level', 'Linked Users', 'Total Deposit Amount', 'Total Deposit Count'])
    #列出日期范围
    for i in range((end - begin).days+1):
        day = begin + datetime.timedelta(days=i)
        strday =str(day)
        csv_date = '%s_%s_%s' % (strday.split('-')[0], strday.split('-')[1], strday.split('-')[2])
        open_df = open_csv(csv_date)
        new_df = cleansing_table(open_df, day)
        df = df.append(new_df) 
        print('Cleasing the csv_file date:'+ csv_date)
    try:
        df.to_csv(r'C:\Users\Jesse\Downloads\Cleasing_data_Deposit Counts_' + csv_date +'.csv', encoding='utf-8-sig', index=False)
        print('Export the file')
    except:
        print('File export failed')