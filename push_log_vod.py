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
df = pd.read_csv(config.filepath_vod, dtype=object)

data = df.loc[(df['PlatformSubGroup'] != 'Box Linux')] #data frame type

data_frame = [] #biến lưu cấu trúc của evtData sau khi format lại(check struct của evtData trong hàm process_row_vod()
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
def convert_to_epoch_time(date_string):
    # Định dạng chuỗi ngày tháng năm
    date_format = "%Y-%m-%d"

    # Chuyển đổi chuỗi thành đối tượng datetime
    datetime_object = datetime.strptime(date_string, date_format)

    # Chuyển đối tượng datetime thành epoch time
    epoch_time = int(datetime_object.timestamp())

    return epoch_time

# -------HÀM CHUYỂN ĐỔI FORMAT PLAYING_SESSION SANG DẠNG EPOCH TIME-------


# ------------------------------------TEMPLATE PUSH VOD--------------------------------------
def process_row_vod(row):
    return {
        "identity": str(row["user_id"]),
        "ts": str(str(row["Date"])),
        "type": 'event',
        "evtName": "Log_User_IDOTT_1",
        "evtData": {
            "device_id": str(row["device_id"]),
            "objectIDIPTV": str(row["ObjectIDIPTV"]),
            "contract": str(row["Contract"]),
            "vodIDIPTV": str(row["VodIDIPTV"]),
            "vodIDOTT": str(row["VodIDOTT"]),
            "title": unidecode(str(row["Title"])),
            "showTime": str(row["ShowTime"]),
            "category": unidecode(str(row["Category"])),
            "subCategory": unidecode(str(row["SubCategory"])),
            "sourceVOD": str(row["SourceVOD"]),
            "contractType": unidecode(str(row["ContractType"])),
            "typeRegister": unidecode(str(row["TypeRegister"])),
            "zone": unidecode(str(row["Zone"])),
            "area": unidecode(str(row["Area"])),
            "chipset": str(row["Chipset"]),
            "platformName": str(row["PlatformName"]),
            "platformSubGroup": str(row["PlatformSubGroup"]),
            "platform_group": str(row["PlatformGroup"]),
            "platform": str(row["platform"]),
            "totalView": str(row["TotalView"]),
            "totalDevice": str(row["TotalDevice"]),
            "totalUser": str(row["TotalUser"]),
            "totalDuration": str(row["TotalDuration"])
        }
    }
# ------------------------------------TEMPLATE PUSH VOD--------------------------------------
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
    #thực hiện apply dữ liệu đọc được từ log detail theo template push data từ hàm process_row_vod vào biến dataframe.
    data_frame = data.apply(process_row_vod, axis=1).tolist()

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
