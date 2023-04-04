from typing import List
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database
from bson import ObjectId

import pandas as pd

from pathlib import Path
import traceback
from datetime import datetime, date, time,timedelta
EXCEL_FILE_INPUT = r"yard_location testing\final_yard_location_01-02.xlsx"
SHEET_NAMES = ["Final_Sheet"]

DB_CONN_STR = "mongodb+srv://testing:testingMAC-56@ty-production.s7emg.mongodb.net/"
# DB_NAMES = ["trailer_yard-01-31-09-41-31"]
DB_NAMES = ["trailer_yard-01-31-10-29-45"]

try:
    conn = MongoClient(DB_CONN_STR)
    print("Connected successfully!!!")
except:
    print("Could not connect to MongoDB")
    exit(0)
    

COLLECTION_NAME = "yard_locations"


def row_colour(color):
    def row_color_(row):
        return ['background-color:'+color for i in row]
    return row_color_

def cell_color(_color):
    def styling_specific_cell(x,row_idx,col_idx):
        color = 'background-color:'+_color
        df_styler = pd.DataFrame('', index=x.index, columns=x.columns)
        df_styler.iloc[row_idx, col_idx] = color
        return df_styler
    return styling_specific_cell 

def get_colored_df(df, color: str, index: List):
    if len(index) > 0:
        # subset = pd.IndexSlice[index, :]
        # if isinstance(df, pd.DataFrame):
        #     df = df.style.apply(row_colour(color),subset=subset)
        # else:
        #     df = df.apply(row_colour(color),subset=subset)
            
        for row_idx, col_idx in index:
            if isinstance(df, pd.DataFrame):
                df = df.style.apply(cell_color(color), row_idx=row_idx, col_idx=col_idx, axis=None)
            else:
                df = df.apply(cell_color(color), row_idx=row_idx, col_idx=col_idx, axis=None)
    return df





def values_from_indexes(list_to_check, indexes):
    final_list = []
    for each_index in indexes:
        final_list.append(list_to_check[each_index] )
    return final_list

        
def get_indexs_non_nan_value(list_to_check):
    final_list = []
    indexes = []
    for i, value in enumerate(list_to_check):
    
        if type(value) != type(1.1) :#or type('gyg') != type(value) :
            indexes.append(i)
    
    for each_index in indexes:
        final_list.append(list_to_check[each_index] )
    return indexes,final_list


def get_location_from_mongo(db: Database, collection: Collection, location_id: str):
    return collection.find_one({"location_id": location_id})





# frames = list(get_all_data())



for DB_NAME in DB_NAMES:
    
    EXCEL_FILE_OUTPUT = "output_{}.xlsx".format(Path(__file__).stem + "__" + DB_NAME + "")
    
    # database
    db = conn[DB_NAME]
    collection_ = db[COLLECTION_NAME]
    
    def get_all_data():
        return collection_.aggregate([
            # {
            #     "$match": {
            #         "frame_number":15601
            #     },
            # },
            # {
            #   "$limit": 1,  
            # },
            {
                "$sort": {"event_time": 1}
            },
        ],allowDiskUse=True)
        
    all_frames = list(get_all_data())
    
    # print(len(all_frames))
    
    for each_frame in all_frames:
        # print(each_frame)
        if 'event_time' in each_frame:
            temp_mongo_event_time = each_frame['event_time']
            break
        # print()
        # break
        

    
    temp_mongo_event_time = temp_mongo_event_time
    
    all_sheets_data = {}
    
    for sheet_name in SHEET_NAMES:
        # read excel
        df = pd.read_excel(EXCEL_FILE_INPUT, sheet_name=sheet_name)
        # print(df.columns)
        
        color_in_df = {}
        try:
            for index, row in df.iterrows():
                # print(f"index: {index}")  
                # print(f"{index = }")

                location_id = row["location_id"]
                time_id = row["time"]
                
                is_not_empty_row = pd.notnull(time_id) and pd.notnull(location_id)
                
                if is_not_empty_row:
                    time_and_loc_index_number = index
                    parking_index_number = index + 1
                    # print('------------')
                    # print()
                    # print("index", time_and_loc_index_number, '====',parking_index_number)

                    # exit()
                    each_loc_and_time_list = df.iloc[time_and_loc_index_number].to_list()
                    parking_or_empty_list = df.iloc[parking_index_number].to_list()
                    
                    # columns_to_color = set()
                    # for i, val in enumerate(each_loc_and_time_list):
                    #     # print(f"{i = } {repr(val) = } {type(val) = }")
                    #     if isinstance(val, time):
                    #         columns_to_color.add(i)
                    # print(f"{columns_to_color = }")
                    
                    # exit()
                    
                    parking_id = int(each_loc_and_time_list[0])
                    times_event_occur_lst = each_loc_and_time_list[1:]
                    parking_occur_lst = parking_or_empty_list[1:]
                    
                    # print(times_event_occur_lst)
                    
                    even_time_indexes , event_time_list = get_indexs_non_nan_value(times_event_occur_lst)
                    # print(even_time_indexes, event_time_list)
                    parking_detail = values_from_indexes(parking_occur_lst, even_time_indexes)
                    
                    # print("parking_id", parking_id)
                    # print(event_time_list)
                    # print(parking_detail)
                    
                    
                    time_loc_temp_detail = {}
                    for each_data_index in range(len(event_time_list)) :
                        time_loc_temp_detail[event_time_list[each_data_index]] = parking_detail[each_data_index]
                        
                
                    each_row_data_dict = {parking_id : time_loc_temp_detail}
                    # print(each_row_data_dict)
                    # continue
                    # exit()
                    for each_frame in all_frames:
                        if 'camera' in each_frame:
                            
                            if str(parking_id) in each_frame.values():
                                # print()
                                
                                temp_dict = {}
                                temp_count = 0
                                # print('*******************', each_frame)
                                # print(f"{each_row_data_dict = }")
                                # print()
                                
                                
                                for k,value in each_row_data_dict.items():
                                    for actual_time, actual_park_status in value.items(): 
                                        # print(f"{parking_id = }")
                                        # print(f"{actual_time =  }")
                                        # print(f"{actual_park_status = }")
                                        
                                        if "event_time" in each_frame :
                                            ts_time = each_frame['event_time'] - timedelta(hours=6)
                                            mongo_parking_status = each_frame['status'].strip()
                                            mongo_event_time = ts_time.time()
                                            
                                            # print(f"{mongo_event_time = }")
                                            # print(f"{mongo_parking_status = }")
                                        
                                        else:
                                            # print("no event time")
                                            ts_time = temp_mongo_event_time - timedelta(hours=6)
                                            mongo_event_time = ts_time.time()  
                                            # print(mongo_event_time, 'mongo_event_time')
                                            mongo_parking_status = 'empty'
                                        
                                        
                                        if actual_time < mongo_event_time:
                                            temp_dict['time'] = actual_time
                                            temp_dict['status'] = actual_park_status
                                            temp_count += 1
                                 
                                # print(temp_dict,'-----------------------------------')    
                                
                                actual_final_time = temp_dict['time']
                                actual_final_park_status = temp_dict['status'].strip()
                                
                                print()
                                print(f"{parking_id = }")
                                print(f"{actual_final_time = }")
                                print(f"{actual_final_park_status = }")
                                print(f"{temp_count = }")
                                print('*****')
                                print(f"{mongo_event_time = }")
                                print(f"{mongo_parking_status = }")
                                
                                
                                # print()
                                
                                if (mongo_parking_status == 'DOCKED' or mongo_parking_status ==  'PARKED') and actual_final_park_status == 'parked':
                                    # print('true')
                                    color_in_df[(index, temp_count)] = "green"
                                #     # for i in columns_to_color:
                                #     #     color_in_df[(index, i)] = "green"
                                elif mongo_parking_status == 'empty' and actual_final_park_status == 'parked':
                                    # print('false')
                                    color_in_df[(index, temp_count)] = "red"
                                        
                                elif mongo_parking_status ==  'empty' and actual_final_park_status == 'empty':
                                    print('in eklsjb')
                                    color_in_df[(index, temp_count)] = "green"
                       
                                
            # print(f"{color_in_df = }")
    #         index_green = [*map(lambda x: x[0],
    #                             filter(lambda x: x[1]=="green", color_in_df.items()))] 
    #         index_pink = [*map(lambda x: x[0],
    #                             filter(lambda x: x[1]=="green", color_in_df.items()))]
    #         index_red = [*map(lambda x: x[0],
    #                             filter(lambda x: x[1]=="red", color_in_df.items()))]
            
                            
    #         df = get_colored_df(df, "green", index_green)
    #         df = get_colored_df(df, "green", index_pink)
    #         df = get_colored_df(df, "red", index_red)            
                      
    #     except Exception as e:
    #         print(e,traceback.format_exc())
                                            
                                           
    # with pd.ExcelWriter(EXCEL_FILE_OUTPUT, mode='w', engine="openpyxl") as writer:
    #         df.to_excel(writer, sheet_name=SHEET_NAMES[0])                                       
                                            
                                            
        #         if index == 6:
        #             break
                                            
        except Exception as e:
            print(e,traceback.format_exc())
                                
                                
                   