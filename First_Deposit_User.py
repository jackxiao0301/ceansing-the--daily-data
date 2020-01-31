import pygsheets
import pandas as pd
import numpy as np
import datetime
import time
import progressbar
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# csv file must be downloaded in English version else the file expot will be failed
def open_csv(start_date, end_date):
    try:
        BG_df = pd.read_csv(
            r'C:\Users\Jesse\Downloads\member_analysis_report_First Deposit Users_' + start_date + '-' + end_date + '.csv',
            encoding='utf-8-sig')
        return BG_df
    except:
        print('File Not Found Error')


def cleansing_table(BG_df):
    BG_df['Source Name'] = BG_df['Source Name'].fillna('')
    Dates = BG_df['First Deposit Date']
    Date = []
    for i in Dates:  # restructed the date
        new_date = i.split(' ')[0].split('/')
        the_date = '%s-%s-%s' % (new_date[2], new_date[0], new_date[1])
        Date.append(the_date)
    DateSer = pd.Series(Date)
    BG_df['First Deposit Date'] = DateSer
    move = BG_df.pop('First Deposit Date')
    BG_df.insert(0, 'First Deposit Date', move)
    return BG_df


def export_csv(BG_df, start_date, end_date):
    try:
        BG_df.to_csv(
            r'C:\Users\Jesse\Downloads\member_analysis_report_First Deposit Users_' + start_date + '-' + end_date + '.csv',
            encoding='utf-8-sig', index=False)
        print('Csv_File exort')
    except:
        print('File export failed')


def get_worksheet_raw_number(auth_json_path, gss_scopes, BG_spreadsheet_key, worksheet_title, start_datetime):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(auth_json_path, gss_scopes)
    gsh = gspread.authorize(credentials)
    try:
        BG_sh = gsh.open_by_key(BG_spreadsheet_key)
    except:
        return 'Goole spreadsheet open failed'
    BG_ws = BG_sh.worksheet(worksheet_title)
    BG_data = BG_ws.get_all_values()
    BG_df = pd.DataFrame(data=BG_data)
    BG_df = BG_df.drop(BG_df.index[:1])
    BG_df[0] = pd.to_datetime(BG_df[0])
    countdate = BG_df[0] < start_datetime
    BG_raw_number = countdate.astype('int').sum()
    return BG_raw_number


def data_append_BGsheet(BG_df, auth_json_path, BG_sheet_url, worksheet_title, BG_raw_number):
    gc = pygsheets.authorize(
        service_account_file=auth_json_path)
    sh = gc.open_by_url(BG_sheet_url)
    ws = sh.worksheet_by_title(worksheet_title)
    new_raw_number = 'A' + str(BG_raw_number + 2)
    ws = ws.set_dataframe(BG_df, new_raw_number)
    return ws


def clean_BGworksheet_raw(auth_json_path, gss_scopes, spreadsheet_key, worksheet_title, BG_raw_number):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(auth_json_path, gss_scopes)
    gsh = gspread.authorize(credentials)
    new_raw_number = BG_raw_number + 2
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
    BG_sheet_url = 'https://docs.google.com/spreadsheets/d/1FCwXc4sHArHLr-_yWUikR8lywYchJw1_g7uoB4LE0ZI/edit#gid=216354897'
    worksheet_title = '历史数据_首存'
    #每次运作必须输入的开始日期以及结束日期
    start_date = input('Date Import（Format YYYY_MM_DD）:')
    end_date = input('Date Import（Format YYYY_MM_DD）:')
    start_datetime = datetime.date(int(start_date.split('_')[0]), int(start_date.split('_')[1]), int(start_date.split('_')[2]))
    BG_df = open_csv(start_date, end_date)
    BG_df = cleansing_table(BG_df)
    try:
        BG_df.to_csv(
            r'C:\Users\Jesse\Downloads\member_analysis_report_First Deposit Users_' + start_date + '-' + end_date + '.csv',
            encoding='utf-8-sig', index=False)
        print('Csv_File exort')
    except:
        print('File export failed')
    star_time = time.time()
    BG_raw_number = get_worksheet_raw_number(auth_json_path, gss_scopes, BG_spreadsheet_key, worksheet_title, start_datetime)
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