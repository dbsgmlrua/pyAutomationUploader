import pymysql
import pandas as pd
from datetime import datetime, timedelta
from sshtunnel import SSHTunnelForwarder
import os
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.http import MediaFileUpload
import time
import schedule
import requests


#Connect DB Using SSHTunnel
def getDataOne(sql):
    with SSHTunnelForwarder(
            (PUT_YOUR_SSH_HOST, 22),
            ssh_username=PUT_YOUR_SSH_NAME,
            ssh_private_key=PUT_YOUR_PEM_KEY_PATH,
            ssh_private_key_password=PUT_YOUR_KEY_PASSWORD,
            remote_bind_address=(PUT_REMOTE_DB_ADRESS, 3306)
        ) as server:
            db = pymysql.connect(host="127.0.0.1", port=server.local_bind_port, user=PUT_DB_USER_NAME, passwd=PUT_DB_PASSWORD, db=PUT_DB_NAME)
            cursor = db.cursor(pymysql.cursors.DictCursor)
            cursor.execute(sql)
            dataresult = cursor.fetchall()
            dataresult = pd.DataFrame(dataresult)
    return dataresult

def getDataTwo(sql):
    with SSHTunnelForwarder(
            (PUT_YOUR_SSH2_HOST, 22),
            ssh_username=PUT_YOUR_SSH2_NAME,
            ssh_private_key=PUT_YOUR_PEM_KEY2_PATH,
            ssh_private_key_password=PUT_YOUR_KEY2_PASSWORD,
            remote_bind_address=(PUT_REMOTE_DB2_ADRESS, 3306)
        ) as server:
            db = pymysql.connect(host="127.0.0.1", port=server.local_bind_port, user=PUT_DB2_USER_NAME, passwd=PUT_DB2_PASSWORD, db=PUT_DB2_NAME)
            cursor = db.cursor(pymysql.cursors.DictCursor)
            cursor.execute(sql)
            dataresult = cursor.fetchall()
            dataresult = pd.DataFrame(dataresult)
    return dataresult

#After uploading send slack message using slack web hook
def slacker(url):
    slackerurl = "https://hooks.slack.com/services/PUT_YOUR_SLACK_WEB_HOOK"
    now = datetime.now()
    datestr = now.strftime("%m월 %d일")
    testtext = datestr + '데이터입니다. : ' + url

    payload = {
        "text": testtext
    }

    requests.post(slackerurl, json=payload)
    print(payload)

#scheduler job
def job():
    now = datetime.now() - timedelta(days=1)
    datestr = now.strftime("%m-%d")
    firstsql ="PUT_YOUR_SELECT_QUERY_FOR_FIRST_DATA"

    #Get first dataframe
    firstresult = getDataOne(firstsql)
    print(firstresult)

    #make second sql query string using first result data
    secondsql ="PUT_YOUR_SELECT_QUERY_FOR_SECOND_DATA"
    
    #second query need first data's key_id
    #make query like ..... WHERE column IN ('key_id1_','key_id_2','key_id_3',......,'key_id_last')
    sqlinstring = "("
    firstrow = True
    for row in firstresult.itertuples():
        if not firstrow:
            sqlinstring = sqlinstring  + "', "
        sqlinstring += "'" + row.key_id
        firstrow = False

    sqlinstring += "')"
    secondsql = secondsql + sqlinstring
    print(secondsql)

    #Get second dataframe
    gpresult = getDataTwo(secondsql)
    print(gpresult)

    #join two dataframe using key "key_id"
    firstresult = firstresult.join(gpresult.set_index('key_id'), on='key_id')
    firstresult = firstresult.drop('key_id', axis=1)

    #my boss want to change NaN to "NULL"
    firstresult = firstresult.fillna(value='NULL')

    print(firstresult)

    #Save dataframe to csv file
    csvfilenameOrigin = 'PUT_YOUR_FILE_NAME' + datestr
    csvfilename = csvfilenameOrigin + '.csv'
    csv_dir = r'./PUT_FILE_PATH'

    csv_path = csv_dir + csvfilename

    filePath = 'PUT_FILE_PATH/'
    filePath = filePath + csvfilename

    firstresult.to_csv(csv_path, index = False, header=True)


    #GOOGLE FILE UPLOADER

    SCOPES = ['https://www.googleapis.com/auth/drive']
    creds = None

    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())


    service = build('drive', 'v3', credentials=creds)
    file_metadata = {
        'name': csvfilenameOrigin,
        'mimeType': 'application/vnd.google-apps.spreadsheet',
        'parents':['PUT_YOUR_GOOGLE_DRIVE_FOLDER_ID']
    }
    media = MediaFileUpload(filePath,
                            mimetype='text/csv',
                            resumable=True)
    file = service.files().create(body=file_metadata,
                                        media_body=media,
                                        fields='id').execute()
    slacker('https://docs.google.com/spreadsheets/d/' + file.get('id'))


if __name__ == '__main__':
    # schedule every day at 2PM
    schedule.every().day.at("14:00").do(job)
    while True:
        now = datetime.now()
        datestr = now.strftime("%m/%d/%Y, %H:%M:%S")
        #check alive
        print("NowRunning : " + datestr)
        schedule.run_pending()
        time.sleep(1)