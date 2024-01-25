import pandas as pd
import matplotlib.pyplot as plt
import random
import json
import numpy as np
from datetime import datetime
import requests
from unidecode import unidecode
import time
import math
from tqdm import tqdm
import re


re_searh = re.compile(r'\.')
# reading log file
df = pd.read_csv('./event_20240101.csv',
                 usecols=["LogUserIDOTT", "playing_session", "EventType", "EventCategory", "League", "Platform",
                          "PlatformGroup", "EventIDOTT", "EventTitle",
                          "ChannelNoOTT", "ChannelName", "ChannelGroup", "RealTimePlaying", "device_id",
                          "SubCompanyNameVN", "LocationNameVN"], dtype=object)

data = df.loc[df.Platform != 'box iptv'].head(600000)  # data frame type

data_frame = [] #biến lưu cấu trúc của evtData sau khi format lại(check struct của evtData trong hàm mappingdata()
# format playing_session
range_list = [(20, 26)]
format = '%Y-%m-%d %H:%M:%S'

# ------------------------------------CLEVER TAP--------------------------------------

# Define các campaign
headers = {
    'X-CleverTap-Account-Id': '4W5-565-W95Z',  # project_id
    'X-CleverTap-Passcode': 'ACO-JIZ-YXKL',  # Passcode
    'Content-Type': 'application/json; charset=utf-8',
}

params = (
    ('batch_size', '150'),
)
# ------------------------------------CLEVER TAP--------------------------------------

# -------1 SỐ HÀM BỔ TRỢ ĐỂ CHUYỂN ĐỔI PLAYING_SESSION SANG DẠNG EPOCH TIME-------
def format_playing_session(time_data):
    res = ""
    if time_data == '0':
        res = "1980-01-01 07:00:00"
    elif time_data is np.nan:
        res = "1980-01-01 07:00:00"
    elif time_data == 'nan':
        res = "1980-01-01 07:00:00"
    else:
        new_str = time_data.replace('T', ' ')
        for idx, chr in enumerate(new_str):
            for strt_idx, end_idx in range_list:
                # checking for ranges and appending
                if strt_idx <= idx + 1 <= end_idx:
                    break
            else:
                res += chr
    return res

def convert_string_to_datetime(str):
    str = datetime.strptime(str, format)
    return str

def search_char(char, text):
    if char in text:
        return 1
    else:
        return 0

def convert_playingsession_to_epochtime(time_data):
    # fomat playin_session
    new_str = ''
    str_format = format_playing_session(time_data)
    # convert string to datetime
    if search_char('-', str_format) == 1:
        str_convert = convert_string_to_datetime(str_format)
        # convert to epochtime
        new_str = datetime.timestamp(str_convert)
    else:
        new_str = time_data
    return new_str

## Hàm kiểm tra xem playing_session có type là Unixpoch hay ko?
def is_unix_epoch(string):
    try:
        timestamp = int(string)
        time.gmtime(timestamp)  # Chuyển đổi thành struct_time để kiểm tra hợp lệ
        return True
    except (ValueError, OverflowError, OSError):
        return False

def convert_unixpoch(time_data):
    timestamp_string = time_data
    # print(timestamp_string)
    try:
        if not is_unix_epoch(timestamp_string):
            # Chuyển đổi thành đối tượng datetime
            timestamp_datetime = datetime.fromtimestamp(int(timestamp_string) / 1000.0)

            # Chuyển đổi thành Unix Epoch
            timestamp_unix_epoch = int(timestamp_datetime.timestamp())

            return timestamp_unix_epoch
        else:
            # print(f"{timestamp_string} không phải là kiểu Unix Epoch.")
            return timestamp_string
    except Exception as e:
        print(e)

# -------1 SỐ HÀM BỔ TRỢ ĐỂ CHUYỂN ĐỔI PLAYING_SESSION SANG DẠNG EPOCH TIME-------


# -----mapping data into dataframe------
def mapping_data():
    for index, row in data.iterrows():
        for i in range(0, len(data)):
            data_frame.append({"identity": str(row["LogUserIDOTT"]),  # rec["user_id"], #LogUserIDOTT
                               "ts": str(convert_unixpoch(convert_playingsession_to_epochtime(str(row["playing_session"])))),
                               "type": 'event',  # row["EventType"],  # EventType.
                               "evtName": "VOD",  # dùng để phân biệt loại content nào VOD, CHANNEL, EVENT.
                               "evtData": {
                                   "platform": row["Platform"],  # platform"
                                   "platform_group": row["PlatformGroup"],  # map_platform(rec["platform"]),
                                   "device_id": row["device_id"],
                                   "event_Category": unidecode(row["EventCategory"]),  # 1
                                   "contentID": row["EventIDOTT"],  # rec["contentID"],  #EventIDOTT
                                   "contentName": unidecode(row["EventTitle"]),  # rec["contentName"], #EventTitle
                                   "channelNoOTT": row["ChannelNoOTT"],
                                   "channelName": unidecode(str(row["ChannelName"])),
                                   "channelGroup": unidecode(str(row["ChannelGroup"])),
                                   "realTimePlaying": row["RealTimePlaying"],
                                   "league": unidecode(str(row["League"])),  # 4
                                   "subCompanyNameVN": unidecode(str(row["SubCompanyNameVN"])),
                                   "locationNameVN": unidecode(str(row["LocationNameVN"])),  # 7
                                   "type": unidecode(str(row["EventType"]))  # EventType #6
                               }
                               })
            break

# def mapping_data():
#     for index, row in data.iterrows():
#         for i in range(0, len(data)):
#             data_frame.append({"identity": row["LogUserIDOTT"],  # rec["user_id"], #LogUserIDOTT
#                                "ts": row["playing_session"], #str(convert_unixpoch(convert_playingsession_to_epochtime(row["playing_session"]))),
#                                "type": 'event',  # row["EventType"],  # EventType.
#                                "evtName": "VOD",  # dùng để phân biệt loại content nào VOD, CHANNEL, EVENT.
#                                "evtData": {
#                                    "platform": row["Platform"],  # platform"
#                                    "platform_group": row["PlatformGroup"],  # map_platform(rec["platform"]),
#                                    "device_id": row["device_id"],
#                                    "event_Category": unidecode(row["EventCategory"]),  # 1
#                                    "contentID": row["EventIDOTT"],  # rec["contentID"],  #EventIDOTT
#                                    "contentName": unidecode(row["EventTitle"]),  # rec["contentName"], #EventTitle
#                                    "channelNoOTT": row["ChannelNoOTT"],
#                                    "channelName": unidecode(str(row["ChannelName"])),
#                                    "channelGroup": unidecode(str(row["ChannelGroup"])),
#                                    "realTimePlaying": row["RealTimePlaying"],
#                                    "league": unidecode(str(row["League"])),  # 4
#                                    "subCompanyNameVN": unidecode(str(row["SubCompanyNameVN"])),
#                                    "locationNameVN": unidecode(str(row["LocationNameVN"])),  # 7
#                                    "type": unidecode(str(row["EventType"]))  # EventType #6
#                                }
#                                })
#             break

# -----mapping data into dataframe------

def main():
    # thực hiện mapping data vào dataframe
    mapping_data()

    #Xử lý mỗi lần gửi sẽ là 1000 record để đảm bảo gửi tất cả data lên clevertap.
    loop_size = 0;
    chunk_size = 1000
    num_chunks = len(data)
    quotient, remainder = divmod(num_chunks, chunk_size)
    print(loop_size)
    if(remainder == 0):
        loop_size = quotient
    else:
        loop_size = quotient + 1

    # Lặp theo loop_size(số lần lặp để gửi tất cả data lên clevertap).
    for i in range(loop_size):
        chunk_start = i * chunk_size
        chunk_end = (i + 1) * chunk_size

        chunk_data = data_frame[chunk_start:chunk_end]

        dict_of_dicts = {'d': chunk_data}  # type dict
        data4 = "{}".format(dict_of_dicts)
        #print(data4)

        #Gửi yêu cầu POST lên clevertap với data=data4
        response = requests.post('https://sg1.api.clevertap.com/1/upload', headers=headers, data=data4)
        print(response.json())

if __name__ == "__main__":
    main()

