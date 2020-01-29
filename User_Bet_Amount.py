import pygsheets
import pandas as pd
import numpy as np
import datetime
import time
import progressbar
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def open_csv(csv_date):
    try:
        df = pd.read_csv(r'C:\Users\Jesse\Downloads\member_analysis_report_User Bet Volume_' + csv_date + '.csv')
        return df
    except:
        print('Read file error')


def cleansing_table(df, day):
    df['GB Bet'] = df['GB Sport Bet Amount']
    df['Lottery Bet'] = df['IBC Sports Lottery Bet Amount'] + df['GB Lottery Bet Amount']
    df['Casino Bet'] = df['Birkin Club Casino Bet Amount'] + df['Emeald Club Casino Bet Amount'] + df[
        'Star Club Casino Bet Amount'] + df['BBIN Club Casino Bet Amount'] + df['Treasure Island Casino Bet Amount'] + \
                       df['New Birkin Casino Bet Amount']
    df['IBC Bet'] = df['IBC Sports Sport Bet Amount']
    df['Electronic Bet'] = df['Emeald Club Electronic Bet Amount'] + df['BBIN Club Electronic Bet Amount'] + df[
        'AG街机，捕鱼 Electronic Bet Amount'] + df['MG Electronic Bet Amount'] + df['Playtech Games Electronic Bet Amount'] + \
                           df['Treasure Island Electronic Bet Amount'] + df['PP Electronic Bet Amount'] + df[
                               'MJ Electronic Bet Amount']
    df['Chess Bet'] = df['KY Chess Bet Amount']
    df = df.drop(['Birkin Club Casino Bet Amount', 'Emeald Club Casino Bet Amount', 'Star Club Casino Bet Amount',
                  'BBIN Club Casino Bet Amount', 'Treasure Island Casino Bet Amount', 'New Birkin Casino Bet Amount',
                  'Emeald Club Electronic Bet Amount', 'BBIN Club Electronic Bet Amount',
                  'AG街机，捕鱼 Electronic Bet Amount', 'MG Electronic Bet Amount', 'Playtech Games Electronic Bet Amount',
                  'Treasure Island Electronic Bet Amount', 'PP Electronic Bet Amount', 'MJ Electronic Bet Amount',
                  'IBC Sports Lottery Bet Amount', 'GB Lottery Bet Amount', 'KY Chess Bet Amount',
                  'GB Sport Bet Amount', 'IBC Sports Sport Bet Amount'], axis=1)
    df.insert(0, 'Date', day)
    return df


def get_worksheet_raw_number(auth_json_path, gss_scopes, BG_spreadsheet_key, worksheet_title):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(auth_json_path, gss_scopes)
    gsh = gspread.authorize(credentials)
    try:
        BG_sh = gsh.open_by_key(BG_spreadsheet_key)
    except:
        return 'Goole spreadsheet open failed'
    BG_ws = BG_sh.worksheet(worksheet_title)
    BG_ws.add_rows(1)
    BG_data = BG_ws.get_all_values()
    BG_df = pd.DataFrame(data=BG_data)
    BG_raw_number = BG_df[0].isnull().count()
    return BG_raw_number


def data_for_sheet(df):
    BG_df = df.drop(['GB Bet', 'Lottery Bet', 'Casino Bet', 'IBC Bet', 'Electronic Bet', 'Chess Bet'], axis=1)
    #Retention_df = df.drop(['Source', 'Source Name', 'Vip Level', 'Risk Level', 'Linked Users'], axis=1)
    return BG_df


def data_append_BGsheet(BG_df, auth_json_path, BG_sheet_url, worksheet_title, BG_raw_number):
    gc = pygsheets.authorize(
        service_account_file=auth_json_path)
    sh = gc.open_by_url(BG_sheet_url)
    ws = sh.worksheet_by_title(worksheet_title)
    new_raw_number = 'A' + str(BG_raw_number + 1)
    ws = ws.set_dataframe(BG_df, new_raw_number)
    return ws


def clean_BGworksheet_raw(auth_json_path, gss_scopes, spreadsheet_key, worksheet_title, BG_raw_number):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(auth_json_path, gss_scopes)
    gsh = gspread.authorize(credentials)
    new_raw_number = BG_raw_number + 1
    try:
        sh = gsh.open_by_key(spreadsheet_key)
    except:
        return 'Goole spreadsheet open failed'
    ws = sh.worksheet(worksheet_title)
    ws.delete_row(int(new_raw_number))
    return ws


if __name__ == '__main__':
    # 预先输入API token位置以及sheet ID 和sheet URL
    auth_json_path = 'C:/Users/Jesse/AppData/Local/Programs/Python/Python38/Scripts/PythonUpload.json'
    gss_scopes = ['https://spreadsheets.google.com/feeds']
    BG_spreadsheet_key = '1FCwXc4sHArHLr-_yWUikR8lywYchJw1_g7uoB4LE0ZI'
    BG_sheet_url = 'https://docs.google.com/spreadsheets/d/1FCwXc4sHArHLr-_yWUikR8lywYchJw1_g7uoB4LE0ZI/edit#gid=1262600911'
    worksheet_title = '历史数据_投注用户'
    #每次运作必须输入的开始日期以及结束日期
    begin = input('Input the begin date(Format:YYYY_MM_DD)')
    end = input('Input the end date(Format:YYYY_MM_DD):')
    barbn = int(begin.split('_')[2])
    baren = int(end.split('_')[2])
    # 转换日期格式
    begin = datetime.date(int(begin.split('_')[0]), int(begin.split('_')[1]), int(begin.split('_')[2]))
    end = datetime.date(int(end.split('_')[0]), int(end.split('_')[1]), int(end.split('_')[2]))
    df = pd.DataFrame(columns=['Date', 'User Name', 'Source', 'Source Name', 'Vip Level', 'Risk Level', 'Linked Users',
                               'Total Bet Amount', 'Total Win Loss Amount', 'Total Turnover Amount', 'GB Bet',
                               'Lottery Bet', 'Casino Bet', 'IBC Bet', 'Electronic Bet', 'Chess Bet'])
    maxv = (baren - barbn + 1)
    bar = progressbar.ProgressBar(max_value=maxv)
    count = 0
    #列出日期范围
    for i in range((end - begin).days + 1):
        day = begin + datetime.timedelta(days=i)
        strday = str(day)
        csv_date = '%s_%s_%s' % (strday.split('-')[0], strday.split('-')[1], strday.split('-')[2])
        open_df = open_csv(csv_date)
        new_df = cleansing_table(open_df, day)
        df = df.append(new_df)
        count += 1
        time.sleep(0.1)
        bar.update(count)
        #print('Cleasing the csv_file date:' + csv_date)
    try:
        df.to_csv(r'C:\Users\Jesse\Downloads\Cleasing_data_User Bet Volume_' + csv_date + '.csv', encoding='utf-8-sig',
                  index=False)
        print('Cleansing all data')
    except:
        print('Data cleansing failed at :' + csv_date)
    star_time = time.time()
    BG_df = data_for_sheet(df)
    BG_raw_number = get_worksheet_raw_number(auth_json_path, gss_scopes, BG_spreadsheet_key, worksheet_title)
    try:
        data_append_BGsheet(BG_df, auth_json_path, BG_sheet_url, worksheet_title, BG_raw_number)
        print('Data uploading to BG done .')
        time.sleep(1)
    except:
        print('Data upload failed')
    try:
        clean_BGworksheet_raw(auth_json_path, gss_scopes, BG_spreadsheet_key, worksheet_title, BG_raw_number)
        end_time = time.time()
        print('Raw data clean done')
        print('Time cost:', end_time-star_time, 's')
    except:
        print('Data clean failed')