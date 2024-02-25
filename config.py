#-----filepath log detail------.
filepath_event = './Data_Event/event_20240219.csv'
filepath_vod = './Data_VOD/vod_20240119.csv'
filepath_channel = './Data_Channel/channel_20240119.csv'


#-----filter theo column trong log detail event-----.
columns_filer_event = ["LogUserIDOTT", "playing_session", "EventType", "EventCategory", "LeagueSeasonNameIPTV",
                       "LeagueRoundNameIPTV", "Platform", "PlatformGroup", "EventIDOTT", "EventTitle", "ChannelNoOTT",
                       "ChannelName", "ChannelGroup", "RealTimePlaying", "device_id", "SubCompanyNameVN", "LocationNameVN"]


#-----header gồm acount_id và passcode của clevertap-----.
headers_clevertap = {
    'X-CleverTap-Account-Id': '4W5-565-W95Z',  #Project_Id
    'X-CleverTap-Passcode': 'ACO-JIZ-YXKL',  #Passcode
    'Content-Type': 'application/json; charset=utf-8',
}







