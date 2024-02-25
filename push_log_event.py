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
df = pd.read_csv(config.filepath_event, usecols=config.columns_filer_event, dtype=object)

data = df.loc[(df['Platform'] != 'box iptv') & (df['LogUserIDOTT'] != "null")] # data frame type

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

# -------HÀM CHUYỂN ĐỔI FORMAT PLAYING_SESSION SANG DẠNG EPOCH TIME-------
def convert_to_datetime(input_data):
    dt_object = datetime.now()

    if not input_data.isdigit() and input_data not in ['nan', '0', 'null', '', ' ']:
        date_formats = [
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S.%f%z",
            "%Y-%m-%d %H:%M:%S"
        ]
        for date_format in date_formats:
            try:
                dt_object = datetime.strptime(input_data, date_format)
                dt = dt_object.strftime('%Y-%m-%d %H:%M:%S')
                input_time = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
                timestamp_string = datetime.timestamp(input_time)
                return timestamp_string
            except ValueError:
                pass

    elif input_data is np.nan or input_data == '0' or input_data == 'nan' or input_data == 'null' or input_data == '' or input_data == ' ':
        timestamp = ""
        return timestamp

    else:
        timestamp_datetime = datetime.fromtimestamp(int(input_data) / 1000.0)
        dt_object = int(timestamp_datetime.timestamp())
        return dt_object

# -------HÀM CHUYỂN ĐỔI FORMAT PLAYING_SESSION SANG DẠNG EPOCH TIME-------

# ------------------------------------TEMPLATE PUSH EVENT--------------------------------------
def process_row(row):
    return {
        "identity": str(row["LogUserIDOTT"]),
        "ts1": row["playing_session"],
        "ts": str(convert_to_datetime(str(row["playing_session"]))),
        "type": 'event',
        "evtName": "Log_User_IDOTT_5",
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
            "leagueSeasonNameIPTV": unidecode(str(row["LeagueSeasonNameIPTV"])),
            "leagueRoundNameIPTV": unidecode(str(row["LeagueRoundNameIPTV"])),
            "subCompanyNameVN": unidecode(str(row["SubCompanyNameVN"])),
            "locationNameVN": unidecode(str(row["LocationNameVN"])),
            "type": unidecode(str(row["EventType"]))
        }
    }
# ------------------------------------TEMPLATE PUSH EVENT--------------------------------------

#-------------------------------------Hàm push data đến CLEVERTAP với 3 tham số chunk_data, headers, semaphore--------------------------
#chunk_data: dữ liệu được gửi lên với dạng là template push event.
# headers: header của clevertap bao gồm account id và passcode.
# semaphore: Semaphore này được sử dụng để kiểm soát số lượng tasks có thể thực hiện đồng thời.

async def send_data_to_clevertap(chunk_data, headers, semaphore):
    data_push = json.dumps({"d": chunk_data})#đây là dữ liệu đc push lên clevertap.
    async with semaphore:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
            async with session.post('https://sg1.api.clevertap.com/1/upload', headers=headers, data=data_push) as response:
                try:
                    response.raise_for_status()  # Nếu có lỗi HTTP, nó sẽ ném một ngoại lệ
                    result = await response.json()
                    print(result)
                except aiohttp.ContentTypeError:
                    print("Server returned content that was not JSON:", await response.text())
                except json.decoder.JSONDecodeError:
                    print("Unable to parse JSON from response.")
                except aiohttp.ClientResponseError as e:
                    print(f"Error in response: {e}")


async def main():
    #thực hiện apply dữ liệu đọc được từ log detail theo template push data từ hàm process_row vào biến dataframe.
    data_frame = data.apply(process_row, axis=1).tolist()

    loop_size = 0 # khởi tạo biến loop_size: biến này đại diện cho số lần lặp để push data.
    chunk_size = 1000 # mỗi lần gửi 1000 record.
    max_concurrent_tasks = 10  # Số lượng task tối đa chạy đồng thời

    #để lấy ra được loop_size thì phải thực hiện lấy ra số dòng của log detail xong dùg hàm divmod nếu chia hết(remainder == 0)
    #thì loop_size = quotient còn chia có dư thì loop_size = quotient + 1
    num_chunks = len(data)
    quotient, remainder = divmod(num_chunks, chunk_size)

    if remainder == 0:
        loop_size = quotient
    else:
        loop_size = quotient + 1
    #------------------------------------------------------

    #Tạo một asyncio.Semaphore với giới hạn số lượng công việc đồng thời để kiểm soát số lượng task.
    semaphore = asyncio.Semaphore(max_concurrent_tasks)
    tasks = []

    #lặp qua loop_size và gửi từng phần dữ liệu cho dến hết.
    for i in range(loop_size):
        chunk_start = i * chunk_size
        chunk_end = (i + 1) * chunk_size
        chunk_data = data_frame[chunk_start:chunk_end]
        task = send_data_to_clevertap(chunk_data, headers, semaphore)
        tasks.append(task)

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
