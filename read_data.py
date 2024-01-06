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

data = df.loc[df.Platform != 'box iptv'].head(3)  # data frame type

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
    ('batch_size', '50'),
)


#response = requests.post('https://sg1.api.clevertap.com/1/upload', headers=headers, params=params, data=data1)


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
            data_frame.append([{'identity': row['LogUserIDOTT'],  # rec["user_id"], #LogUserIDOTT
                                'ts': str(convert_playingsession_to_epochtime(row['playing_session'])),
                                # int(float(rec["playStartTime"])) / 1000, #playing_session (xu ly theo dang epoch)
                                'type': row['EventType'],  # EventType
                                "evtName": "Play_event",  # dùng để phân biệt loại content nào VOD, CHANNEL, EVENT.
                                'evtData': {
                                    'eventCategory': row['EventCategory'],
                                    'league': row['League'],
                                    'platform': row['Platform'],  # platform"
                                    'platform_group': row['PlatformGroup'],  # map_platform(rec["platform"]),
                                    'contentID': row['EventIDOTT'],  # rec["contentID"],  #EventIDOTT
                                    'contentName': row['EventTitle'],  # rec["contentName"], #EventTitle
                                    'channelNoOTT': row['ChannelNoOTT'],
                                    'channelName': row['ChannelName'],
                                    'channelGroup': row['ChannelGroup'],
                                    'realTimePlaying': row['RealTimePlaying'],
                                    'device_id': row['device_id'],
                                    'subCompanyNameVN': row['SubCompanyNameVN'],
                                    'locationNameVN': row['LocationNameVN']
                                }
                                }])
            break

mapping_data()

#print(data_frame)

new_data_frame = str(data_frame)

dict1 = {}
dict1["d"] = new_data_frame
data3 = str(dict1)

#print(data3)

response = requests.post('https://sg1.api.clevertap.com/1/upload', headers=headers, params=params, data=data3)

print(response)
print(response.json())

# for index, row in data.iterrows():
#     print(row['LogUserIDOTT'], row["playing_session"], convert_playingsession_to_epochtime((row['playing_session'])))


# for index, row in data.iterrows():
#     print(row["playing_session"], row['device_id'] , row['LocationNameVN'])


# -------list--------
# for i in data_frame:
#     print(i.get('evtData')['eventCategory'])
# ----function isstance-------
# isinstance(i,dict)

