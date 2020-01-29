import pygsheets
import pandas as pd
import numpy as np
import datetime
import time
import progressbar
import gspread
from oauth2client.service_account import ServiceAccountCredentials


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
    BG_df = df.fillna('')
    return BG_df


def get_worksheet_raw_number(auth_json_path, gss_scopes, BG_spreadsheet_key, worksheet_title_Actuser, worksheet_title_Actcontruct):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(auth_json_path, gss_scopes)
    gsh = gspread.authorize(credentials)
    try:
        BG_sh = gsh.open_by_key(BG_spreadsheet_key)
    except:
        return 'Goole spreadsheet open failed'
    BG_ws = BG_sh.worksheet(worksheet_title_Actuser)
    BG_ws.add_rows(1)
    BG_data = BG_ws.get_all_values()
    BG_df = pd.DataFrame(data=BG_data)
    BG_Actuser_raw_number = BG_df[0].isnull().count()
    BG1_ws = BG_sh.worksheet(worksheet_title_Actcontruct)
    BG1_ws.add_rows(1)
    BG1_data = BG1_ws.get_all_values()
    BG1_df = pd.DataFrame(data=BG1_data)
    BG_Actcontruct_raw_number = BG1_df[0].isnull().count()
    return BG_Actuser_raw_number, BG_Actcontruct_raw_number


def data_append_BGsheet(BG_df, auth_json_path, BG_sheet_url, worksheet_title_Actuser, BG_Actuser_raw_number, BG_Actcontruct_df, worksheet_title_Actcontruct, BG_Actcontruct_raw_number):
    gc = pygsheets.authorize(
        service_account_file=auth_json_path)
    sh = gc.open_by_url(BG_sheet_url)
    ws = sh.worksheet_by_title(worksheet_title_Actuser)
    new_raw_number_Act = 'A' + str(BG_Actuser_raw_number + 1)
    ws = ws.set_dataframe(BG_df, new_raw_number_Act)
    ws1 = sh.worksheet_by_title(worksheet_title_Actcontruct)
    new_raw_number_Act = 'A' + str(BG_Actcontruct_raw_number + 1)
    ws1 = ws1.set_dataframe(BG_Actcontruct_df, new_raw_number_Act)
    return ws, ws1


def clean_BGworksheet_raw(auth_json_path, gss_scopes, BG_spreadsheet_key, worksheet_title_Actuser, BG_Actuser_raw_number, worksheet_title_Actcontruct, BG_Actcontruct_raw_number):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(auth_json_path, gss_scopes)
    gsh = gspread.authorize(credentials)
    new_act_raw_number = BG_Actuser_raw_number + 1
    new_contruct_raw_number = BG_Actcontruct_raw_number +1
    try:
        sh = gsh.open_by_key(BG_spreadsheet_key)
    except:
        return 'Goole spreadsheet open failed'
    ws = sh.worksheet(worksheet_title_Actuser)
    ws.delete_row(int(new_act_raw_number))
    ws1 = sh.worksheet(worksheet_title_Actcontruct)
    ws1.delete_row(int(new_contruct_raw_number))
    return ws, ws1


def data_for_sheet(BG_df):
    BG_Actcontruct_df = BG_df.drop(['Vip Level', 'Risk Level', 'Verification Status', 'Total Deposit Amount', 'Total Withdrawal Amount', 'Total Bonus Amount'], axis=1)
    #Retention_df = df.drop(['Source', 'Source Name', 'Vip Level', 'Risk Level', 'Linked Users'], axis=1)
    return BG_Actcontruct_df, BG_df


if __name__ == '__main__':
    # 预先输入API token位置以及sheet ID 和sheet URL
    auth_json_path = 'C:/Users/Jesse/AppData/Local/Programs/Python/Python38/Scripts/PythonUpload.json'
    gss_scopes = ['https://spreadsheets.google.com/feeds']
    BG_spreadsheet_key = '1FCwXc4sHArHLr-_yWUikR8lywYchJw1_g7uoB4LE0ZI'
    BG_sheet_url = 'https://docs.google.com/spreadsheets/d/1FCwXc4sHArHLr-_yWUikR8lywYchJw1_g7uoB4LE0ZI/edit#gid=847692347'
    worksheet_title_Actuser = '历史数据_活跃用户'
    worksheet_title_Actcontruct = '历史数据_活跃用户构成'
    #每次运作必须输入的开始日期以及结束日期
    begin = input('Input the begin date(Format:YYYY_MM_DD)')
    end = input('Input the end date(Format:YYYY_MM_DD):')
    barbn = int(begin.split('_')[2])
    baren = int(end.split('_')[2])
    begin = datetime.date(int(begin.split('_')[0]),int(begin.split('_')[1]),int(begin.split('_')[2]))
    end = datetime.date(int(end.split('_')[0]),int(end.split('_')[1]),int(end.split('_')[2]))
    BG_df = pd.DataFrame(columns=['Date', 'User Name', 'Source', 'Source Name', 'Vip Level', 'Risk Level', 'Verification Status', 'Total Deposit Amount', 'Total Withdrawal Amount', 'Total Bonus Amount'])
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
        BG_df = BG_df.append(new_df)
        count += 1
        time.sleep(0.1)
        bar.update(count)
        #print('Cleasing the csv_file date:'+ csv_date)
    try:
        BG_df.to_csv(r'C:\Users\Jesse\Downloads\Cleasing_data_Active Users' + csv_date +'.csv', encoding='utf-8-sig', index=False)
        print('Export the file')
    except:
        print('File export failed at:' + csv_date)
    star_time = time.time()
    BG_Actuser_raw_number, BG_Actcontruct_raw_number = get_worksheet_raw_number(auth_json_path, gss_scopes, BG_spreadsheet_key, worksheet_title_Actuser, worksheet_title_Actcontruct)
    BG_Actcontruct_df, BG_df = data_for_sheet(BG_df)
    try:
        data_append_BGsheet(BG_df, auth_json_path, BG_sheet_url, worksheet_title_Actuser, BG_Actuser_raw_number, BG_Actcontruct_df, worksheet_title_Actcontruct, BG_Actcontruct_raw_number)
        print('Data uploading to BG done .')
        time.sleep(1)
    except:
        print('Data upload failed')
    try:
        clean_BGworksheet_raw(auth_json_path, gss_scopes, BG_spreadsheet_key, worksheet_title_Actuser, BG_Actuser_raw_number, worksheet_title_Actcontruct, BG_Actcontruct_raw_number)
        end_time = time.time()
        print('Raw data clean done')
        print('Time cost:', end_time-star_time, 's')
    except:
        print('Data clean failed')