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
import aiohttp
import asyncio
import config

# reading log file
df = pd.read_csv(config.filepath, usecols=config.columns_filer, dtype=object)

data = df.loc[df.Platform != 'box iptv']  # data frame type

data_frame = [] #biến lưu cấu trúc của evtData sau khi format lại(check struct của evtData trong hàm mappingdata()
# format playing_session
range_list = [(20, 26)]
format = '%Y-%m-%d %H:%M:%S'

# ------------------------------------CLEVER TAP--------------------------------------

# Define các campaign
headers = config.headers_clevertap

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
            return timestamp_string
    except Exception as e:
        print(e)

# -------1 SỐ HÀM BỔ TRỢ ĐỂ CHUYỂN ĐỔI PLAYING_SESSION SANG DẠNG EPOCH TIME-------

def process_row(row):
    return {
        "identity": str(row["LogUserIDOTT"]),
        "ts": str(convert_unixpoch(convert_playingsession_to_epochtime(str(row["playing_session"])))),
        "type": 'event',
        "evtName": "Log_User_IDOTT",
        "evtData": {
            "platform": row["Platform"],
            "platform_group": row["PlatformGroup"],
            "device_id": row["device_id"],
            "event_Category": unidecode(row["EventCategory"]),
            "contentID": row["EventIDOTT"],
            "contentName": unidecode(row["EventTitle"]),
            "channelNoOTT": row["ChannelNoOTT"],
            "channelName": unidecode(str(row["ChannelName"])),
            "channelGroup": unidecode(str(row["ChannelGroup"])),
            "realTimePlaying": row["RealTimePlaying"],
            "league": unidecode(str(row["League"])),
            "subCompanyNameVN": unidecode(str(row["SubCompanyNameVN"])),
            "locationNameVN": unidecode(str(row["LocationNameVN"])),
            "type": unidecode(str(row["EventType"]))
        }
    }

async def send_data_to_clevertap(chunk_data, headers, semaphore):
    data4 = json.dumps({"d": chunk_data})
    async with semaphore:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
            async with session.post('https://sg1.api.clevertap.com/1/upload', headers=headers, data=data4) as response:
                try:
                    response.raise_for_status()  # Nếu có lỗi HTTP, nó sẽ ném một ngoại lệ
                    result = await response.json()
                    print(result)
                    #print(data4)
                except aiohttp.ContentTypeError:
                    print("Server trả về nội dung không phải là JSON:", await response.text())
                except json.decoder.JSONDecodeError:
                    print("Không thể phân tích JSON từ phản hồi.")
                except aiohttp.ClientResponseError as e:
                    print(f"Error in response: {e}")



async def main():
    data_frame = data.apply(process_row, axis=1).tolist()

    loop_size = 0
    chunk_size = 1000
    max_concurrent_tasks = 10  # Số lượng task tối đa chạy đồng thời

    num_chunks = len(data)
    quotient, remainder = divmod(num_chunks, chunk_size)

    if remainder == 0:
        loop_size = quotient
    else:
        loop_size = quotient + 1

    print(loop_size)
    semaphore = asyncio.Semaphore(max_concurrent_tasks)
    tasks = []

    for i in range(loop_size):
        chunk_start = i * chunk_size
        chunk_end = (i + 1) * chunk_size
        chunk_data = data_frame[chunk_start:chunk_end]
        task = send_data_to_clevertap(chunk_data, headers, semaphore)
        tasks.append(task)

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
