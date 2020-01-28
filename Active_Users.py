import pandas as pd
import numpy as np
import datetime
import time
import progressbar 


#数据下载的格式必须是英文
def open_csv(csv_date):
    try:
        df = pd.read_csv(r'C:\Users\Jesse\Downloads\member_analysis_report_Active Users_' + csv_date + '.csv', encoding='utf-8-sig')
        return df
    except:
        print('File Not Found Error')


def cleasing_table(df, day):
    #删除‘Last Bet Time’、‘Total Bet Amount’、‘Balance’、‘Last Bet Provider’
    df = df.drop(['Last Bet Time', 'Total Bet Amount', 'Balance', 'Last Bet Provider'], axis=1)
    df.insert(0, 'Date', day)
    move = df.pop('Total Bonus Amount')
    df = pd.concat([df, move], 1)
    df = df.fillna('')
    return df
        

if __name__ == '__main__':
    begin = input('Input the begin date(Format:YYYY_MM_DD)')
    end = input('Input the end date(Format:YYYY_MM_DD):')
    barbn = int(begin.split('_')[2])
    baren = int(end.split('_')[2])
    begin = datetime.date(int(begin.split('_')[0]),int(begin.split('_')[1]),int(begin.split('_')[2]))
    end = datetime.date(int(end.split('_')[0]),int(end.split('_')[1]),int(end.split('_')[2]))
    df = pd.DataFrame(columns=['Date', 'User Name', 'Source', 'Source Name', 'Vip Level', 'Risk Level', 'Verification Status', 'Total Deposit Amount', 'Total Withdrawal Amount', 'Total Bonus Amount'])
    maxv = (baren - barbn + 1)
    bar = progressbar.ProgressBar(max_value=maxv)
    count = 0
    #列出日期范围
    for i in range((end - begin).days+1):
        day = begin + datetime.timedelta(days=i)
        strday =str(day)
        csv_date = '%s_%s_%s' % (strday.split('-')[0], strday.split('-')[1], strday.split('-')[2])
        open_df = open_csv(csv_date)
        new_df = cleasing_table(open_df, day)
        df = df.append(new_df)
        count += 1 
        time.sleep(0.1)
        bar.update(count) 
        #print('Cleasing the csv_file date:'+ csv_date)
    try:
        df.to_csv(r'C:\Users\Jesse\Downloads\Cleasing_data_Active Users' + csv_date +'.csv', encoding='utf-8-sig', index=False)
        print('Export the file')
    except:
        print('File export failed at:' + csv_date)