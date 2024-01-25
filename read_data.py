import pandas as pd
import matplotlib.pyplot as plt
import random
import json
from datetime import datetime
import requests
from unidecode import unidecode
import time

# reading log file
df = pd.read_csv('./event_20240101.csv',
                 usecols=["LogUserIDOTT", "playing_session", "EventType", "EventCategory", "League", "Platform",
                          "PlatformGroup", "EventIDOTT", "EventTitle",
                          "ChannelNoOTT", "ChannelName", "ChannelGroup", "RealTimePlaying", "device_id",
                          "SubCompanyNameVN", "LocationNameVN"], dtype=object)

data = df.loc[df.Platform != 'box iptv'].head(100)  # data frame type

data_frame = []
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
    ('batch_size', '50'),
)


def format_playing_session(str):
    new_str = str.replace('T', ' ')
    res = ""
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

def convert_playingsession_to_epochtime(str):
    # fomat playin_session
    substr = 'T'
    new_str = ''
    if substr in str:
        str_format = format_playing_session(str)
        # convert string to datetime
        str_convert = convert_string_to_datetime(str_format)
        # convert to epochtime
        new_str = datetime.timestamp(str_convert)
        return new_str
    else:
        return str

## Hàm kiểm tra xem playing_session có type là Unixpoch hay ko?
def is_unix_epoch(string):
    try:
        timestamp = int(string)
        time.gmtime(timestamp)  # Chuyển đổi thành struct_time để kiểm tra hợp lệ
        return True
    except (ValueError, OverflowError, OSError):
        return False

def convert_unixpoch(string):
    timestamp_string = string
    # print(timestamp_string)

    if (is_unix_epoch(timestamp_string) == False):
        # Chuyển đổi thành đối tượng datetime
        timestamp_datetime = datetime.fromtimestamp(int(timestamp_string) / 1000.0)

        # Chuyển đổi thành Unix Epoch
        timestamp_unix_epoch = int(timestamp_datetime.timestamp())

        return timestamp_unix_epoch
    else:
        # print(f"{timestamp_string} không phải là kiểu Unix Epoch.")
        return timestamp_string


# -----mapping data into dataframe------
def mapping_data():
    for index, row in data.iterrows():
        for i in range(0, len(data)):
            data_frame.append({"identity": row["LogUserIDOTT"],  # rec["user_id"], #LogUserIDOTT
                               "ts": str(convert_unixpoch(convert_playingsession_to_epochtime(row["playing_session"]))),
                               "type": 'event',  # row["EventType"],  # EventType
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


def main():
    mapping_data()

    # Xử lý số lần lặp để đảm bảo gửi tất cả data lên clevertap.
    loop_size = 0;
    chunk_size = 10
    num_chunks = len(data)
    quotient, remainder = divmod(num_chunks, chunk_size)

    if(remainder == 0):
        loop_size = quotient
    else:
        loop_size = quotient + 1

    print(loop_size)

    # Gửi từng phần dữ liệu
    for i in range(loop_size):
        chunk_start = i * chunk_size
        chunk_end = (i + 1) * chunk_size

        chunk_data = data_frame[chunk_start:chunk_end]

        dict_of_dicts = {'d': chunk_data}  # type dict
        data4 = "{}".format(dict_of_dicts)
        print(data4)

        #Gửi yêu cầu POST với data4
        response = requests.post('https://sg1.api.clevertap.com/1/upload', headers=headers,
                                 data=data4)

        #Xử lý lỗi và thực hiện lại nếu cần
        if response.status_code != 200:
            print(response.json())
        else:
            print(response.json())

if __name__ == "__main__":
    main()

