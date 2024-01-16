import pandas as pd
import matplotlib.pyplot as plt
import random
import json
from datetime import datetime
import requests

#reading log file
df = pd.read_csv('./event_20240101.csv',
                 usecols=["LogUserIDOTT", "playing_session", "EventType", "EventCategory", "League", "Platform",
                          "PlatformGroup", "EventIDOTT", "EventTitle",
                          "ChannelNoOTT", "ChannelName", "ChannelGroup", "RealTimePlaying", "device_id",
                          "SubCompanyNameVN", "LocationNameVN"], dtype=object)

data = df.loc[df.Platform != 'box iptv'].head(4)  # data frame type

data_frame = []
# format playing_session
range_list = [(20, 26)]
format = '%Y-%m-%d %H:%M:%S'

#------------------------------------CLEVER TAP--------------------------------------

# Define các campaign
headers =  {
    'X-CleverTap-Account-Id': '4W5-565-W95Z',  # project_id
    'X-CleverTap-Passcode': 'ACO-JIZ-YXKL',  # Passcode
    'Content-Type': 'application/json; charset=utf-8',
}

params = (
    ('batch_size', '100'),
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

# -----mapping data------

def mapping_data():
    for index, row in data.iterrows():
        for i in range(0, len(data)):
            data_frame.append({"identity": row["LogUserIDOTT"],  # rec["user_id"], #LogUserIDOTT
                                "ts": str(convert_playingsession_to_epochtime(row["playing_session"])),
                                "type": 'event', #row["EventType"],  # EventType
                                "evtName": "VOD",  # dùng để phân biệt loại content nào VOD, CHANNEL, EVENT.
                                "evtData": {
                                     "platform": row["Platform"],  # platform"
                                     "platform_group": row["PlatformGroup"],  # map_platform(rec["platform"]),
                                     "device_id": row["device_id"],
                                     #"eventCategory": row["EventCategory"],
                                     "contentID": row["EventIDOTT"],  # rec["contentID"],  #EventIDOTT
                                     #"contentName": row["EventTitle"],  # rec["contentName"], #EventTitle
                                     "channelNoOTT": row["ChannelNoOTT"],
                                     #"channelName": row["ChannelName"],
                                     #"channelGroup": row["ChannelGroup"],
                                     "realTimePlaying": row["RealTimePlaying"],
                                     #"league": row["League"],
                                     #"subCompanyNameVN": row["SubCompanyNameVN"],
                                     #"locationNameVN": row["LocationNameVN"],
                                     #"type": row["EventType"]  # EventType
                                }
                                })
            break


def main():
    mapping_data()
    new_data_frame = str(data_frame)
    dict_of_dicts = {'d': data_frame}
    data4 = "{}".format(dict_of_dicts)
    print(data4)
    #print(type(data4))

    #pushing log
    response = requests.post('https://sg1.api.clevertap.com/1/upload', headers=headers, data=data4)
    print(response)
    print(response.json())


if __name__ == "__main__":
    main()





# for index, row in data.iterrows():
#     print(row['LogUserIDOTT'], row["EventType"])






