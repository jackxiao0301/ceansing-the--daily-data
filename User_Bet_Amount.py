import pandas as pd
import numpy as np 
import datetime


def open_csv(csv_date):
    try:
        df = pd.read_csv(r'C:\Users\Jesse\Downloads\member_analysis_report_User Bet Volume_' + csv_date + '.csv')
        return df
    except:
        print('Read file error')


def cleansing_table(df, day):
    df['GB Bet'] = df['GB Sport Bet Amount']
    df['Lottery Bet'] = df['IBC Sports Lottery Bet Amount'] + df['GB Lottery Bet Amount']
    df['Casino Bet'] = df['Birkin Club Casino Bet Amount'] + df['Emeald Club Casino Bet Amount'] + df['Star Club Casino Bet Amount'] + df['BBIN Club Casino Bet Amount'] + df['Treasure Island Casino Bet Amount'] + df['New Birkin Casino Bet Amount']
    df['IBC Bet'] = df['IBC Sports Sport Bet Amount']
    df['Electronic Bet'] = df['Emeald Club Electronic Bet Amount'] + df['BBIN Club Electronic Bet Amount'] + df['AG街机，捕鱼 Electronic Bet Amount'] + df['MG Electronic Bet Amount'] + df['Playtech Games Electronic Bet Amount'] + df['Treasure Island Electronic Bet Amount'] + df['PP Electronic Bet Amount'] + df['MJ Electronic Bet Amount']
    df['Chess Bet'] = df['KY Chess Bet Amount']
    df = df.drop(['Birkin Club Casino Bet Amount', 'Emeald Club Casino Bet Amount', 'Star Club Casino Bet Amount', 'BBIN Club Casino Bet Amount', 'Treasure Island Casino Bet Amount', 'New Birkin Casino Bet Amount', 'Emeald Club Electronic Bet Amount', 'BBIN Club Electronic Bet Amount', 'AG街机，捕鱼 Electronic Bet Amount', 'MG Electronic Bet Amount', 'Playtech Games Electronic Bet Amount', 'Treasure Island Electronic Bet Amount', 'PP Electronic Bet Amount', 'MJ Electronic Bet Amount', 'IBC Sports Lottery Bet Amount', 'GB Lottery Bet Amount', 'KY Chess Bet Amount', 'GB Sport Bet Amount','IBC Sports Sport Bet Amount'], axis=1)
    df.insert(0, 'Date', day)
    return df


if __name__ == '__main__':
    begin = input('Input the begin date(Format:YYYY_MM_DD)')
    end = input('Input the end date(Format:YYYY_MM_DD):')
    begin = datetime.date(int(begin.split('_')[0]),int(begin.split('_')[1]),int(begin.split('_')[2]))
    end = datetime.date(int(end.split('_')[0]),int(end.split('_')[1]),int(end.split('_')[2]))
    df = pd.DataFrame(columns = ['Date', 'User Name', 'Source', 'Source Name', 'Vip Level', 'Risk Level', 'Linked Users', 'Total Bet Amount', 'Total Win Loss Amount', 'Total Turnover Amount', 'GB Bet', 'Lottery Bet', 'Casino Bet', 'IBC Bet', 'Electronic Bet', 'Chess Bet'])
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
        df.to_csv(r'C:\Users\Jesse\Downloads\Cleasing_data_User Bet Volume_' + csv_date +'.csv', encoding='utf-8-sig', index=False)
        print('Export the file')
    except:
        print('File export failed')