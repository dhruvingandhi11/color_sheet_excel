from __future__ import print_function

import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pymongo import MongoClient
# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1bIU58SAgnGWehz6VsIuaGk3fQp68tjlTonSqtUeni_g'
SAMPLE_RANGE_NAME = 'CAM14-AKSH'

spread_sheets=[{
    "sheet_name":"CAM14-AKSH",
    "sheet_id":235248026
},{
    "sheet_name":"CAM7-DGSD",
    "sheet_id":0
},{
    "sheet_name":"CAM15-SD",
    "sheet_id":1989478833
},{
    "sheet_name":"CAM16-DG",
    "sheet_id":806227521
},{
    "sheet_name":"CAM17-SD",
    "sheet_id":702801876
},{
    "sheet_name":"CAM18-AKSH",
    "sheet_id":1614628006
}]
# SAMPLE_RANGE_NAME = None

#MongoDB
url="mongodb://192.168.44.170:27017/"
conn=MongoClient(url)
db=conn["fnp-staging"]
# suffix="-2023-02-14-10"
suffix=""
mate_unmate_event_logs=db["mate_unmate_event_logs"+suffix]
lpn_pallets=db["lpn_pallets"+suffix]

def main():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
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

    try:
        service = build('sheets', 'v4', credentials=creds)
        update_requests=[]
        write_update_requests=[]
        # Call the Sheets API
        for spread_sheet in spread_sheets:
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                        range=spread_sheet["sheet_name"]+"!A:H").execute()
            values = result.get('values', [])

            if not values:
                print('No data found.')
                return

            print(len(values))
            
            for i,row in enumerate(values):
                # Print columns A and E, which correspond to indices 0 and 4.
                write_update_requests.append({
                            "range": spread_sheet["sheet_name"]+"!I"+str(i+1)+":"+"I"+str(i+1),
                            "majorDimension": "ROWS",
                            "values": [
                                [""],
                            ],
                        })
                if len(row)==0 or len(row)<6 or (row[0] == "camera" or row[0] == ""):
                    continue
                # print('%s' % (row))

                pallet_id=row[3]
                camera_id=row[0]
                location=row[6]
                forklift_april_tag=row[1]

                background_color={
                    "backgroundColor": {
                            "red":236/255,
                            "green": 217/255,
                            "blue": 221/255,
                            "alpha":1
                        }
                }
                #journey id default value
                
                if forklift_april_tag=="U":
                    background_color={
                            "backgroundColor": {
                                    "red": 255/255,
                                    "green": 255/255,
                                    "blue": 159/255,
                                    "alpha": 1

                                }
                        }
                    							 
                elif forklift_april_tag=="WR" or forklift_april_tag=="RR":
                    event=lpn_pallets.find_one({"current_pallet":pallet_id,"camera_id":camera_id,"location":location})
                    if event:
                        background_color={
                            "backgroundColor": {
                                    "red": 160/255,
                                    "green": 247/255,
                                    "blue": 197/255,
                                    "alpha": 1

                                }
                        }
                        # print("found")
                else:
                    event=mate_unmate_event_logs.find_one({
                        "$or":[
                            {"pallet_tracking_tag":pallet_id},
                            {"pallet_all_ids":pallet_id}
                        ],
                        
                        "event_camera":camera_id,"event_location":location})
                    # if pallet_id=="882":
                    #     print(event)
                    if event:
                        background_color={
                            "backgroundColor": {
                                    "red": 200/255,
                                    "green": 247/255,
                                    "blue": 197/255,
                                    "alpha": 1

                                }
                        }
                        if "journey_id" in event:
                            write_update_requests.append({
                                "range": spread_sheet["sheet_name"]+"!I"+str(i+1)+":"+"I"+str(i+1),
                                "majorDimension": "ROWS",
                                "values": [
                                    [str(event["journey_id"])],
                                ],
                            })
                        
               
                   
                
                update_requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId":spread_sheet["sheet_id"] ,
                            "startRowIndex": i,
                            "endRowIndex": i+1,
                            "startColumnIndex": 0,
                            "endColumnIndex": 7,
                        },
                        "cell": {
                            "userEnteredFormat": background_color
                            
                        },
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                })

            # print(update_requests)
        if len(update_requests)>0:
            
            batch_update_spreadsheet_request_body = {
                "requests":update_requests
            }

            request = service.spreadsheets().batchUpdate(spreadsheetId=SAMPLE_SPREADSHEET_ID, body=batch_update_spreadsheet_request_body)
            res = request.execute()
        
        if len(write_update_requests)>0:

            batch_write_update_spreadsheet_request_body = {
                "value_input_option": "RAW", 
                "data":write_update_requests
            }

            request = service.spreadsheets().values().batchUpdate(spreadsheetId=SAMPLE_SPREADSHEET_ID, body=batch_write_update_spreadsheet_request_body)
            res = request.execute()

    except HttpError as err:
        print(err)


if __name__ == '__main__':
    main()