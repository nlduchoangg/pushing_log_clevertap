
filepath = './event_20240101.csv'
columns_filer = ["LogUserIDOTT", "playing_session", "EventType", "EventCategory", "League", "Platform", "PlatformGroup", "EventIDOTT", "EventTitle", "ChannelNoOTT", "ChannelName", "ChannelGroup", "RealTimePlaying", "device_id", "SubCompanyNameVN", "LocationNameVN"]

headers_clevertap = {
    'X-CleverTap-Account-Id': '4W5-565-W95Z',  # project_id
    'X-CleverTap-Passcode': 'ACO-JIZ-YXKL',  # Passcode
    'Content-Type': 'application/json; charset=utf-8',
}

