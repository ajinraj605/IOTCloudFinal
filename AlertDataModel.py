import datetime
import time
import json
from Database import Database

# Get the service resource.
import Database

#This function is used to detect the anomaly.
#It will send the detected anomaly to Database.py to upload in dynamoDB
#It will print the detected amomaly in the console
def anomaly_detector(dev_id):
    # Reading the configuration file
    f = open("config_alert.json")
    config = json.loads(f.read())
    f.close()

    #Getting the aggregated data from Database.py
    table_name = 'Agg_Table'
    items = Database.Database.get_data(table_name, dev_id)

    #creating all counters
    Heart_Max_Count = 0
    Heart_Min_Count = 0
    Temp_Max_Count = 0
    Temp_Min_Count = 0
    Final_dic = {}

    # For loop for the Agg_data, which is in a list.
    for i in range(0, len(items)):

        if items[i]['datatype'] == 'HeartRate':
            if items[i]['Average'] > config['Rule_1']['avg_max']:
                Heart_Min_Count = 0
                Heart_Max_Count = Heart_Max_Count + 1
                if Heart_Max_Count == 1:
                    Final_dic["Heart_Max_data"] = items[i]
                elif Heart_Max_Count == config["Rule_1"]["trigger_count"]:
                    Final_dic["Heart_Max_data"]["Rule"] = config["Rule_1"]["Rule_num"]
                    Final_dic["Heart_Max_data"]["Alert"] = config["Rule_1"]["max_alert"]

                    # Anomaly data will be sent to Database.py to upload it to dynamoDB
                    Database.Database.upload_anomaly_data(Final_dic["Heart_Max_data"])
                    data = Final_dic["Heart_Max_data"]

                    # Anomaly data will be printer here
                    print('Alert for device_id', data['deviceid'], 'on rule', data['Rule'], 'starting at',  data['timestamp'], 'with the alert type',  data['Alert'])
                    Heart_Max_Count = 0
                    Final_dic.pop("Heart_Max_data")

            elif items[i]['Average'] < config['Rule_1']['avg_min']:
                Heart_Max_Count = 0
                Heart_Min_Count = Heart_Min_Count + 1
                if Heart_Min_Count == 1:
                    Final_dic["Heart_Min_data"] = items[i]
                elif Heart_Min_Count == config["Rule_1"]["trigger_count"]:
                    Final_dic["Heart_Min_data"]["Rule"] = config["Rule_1"]["Rule_num"]
                    Final_dic["Heart_Min_data"]["Alert"] = config["Rule_1"]["min_alert"]

                    # Anomaly data will be sent to Database.py to upload it to dynamoDB
                    Database.Database.upload_anomaly_data(Final_dic["Heart_Min_data"])
                    data = Final_dic["Heart_Min_data"]
                    print('Alert for device_id', data['deviceid'], 'on rule', data['Rule'], 'starting at',
                          data['timestamp'], 'with the alert type', data['Alert'])
                    Heart_Min_Count = 0
                    Final_dic.pop("Heart_Min_data")

            # Making all counter 0 if the reading is within the limit
            else:
                Heart_Max_Count = 0
                Heart_Min_Count = 0

        if items[i]['datatype'] == 'Temperature':
            if items[i]['Average'] > config['Rule_2']['avg_max']:
                Temp_Min_Count = 0
                Temp_Max_Count = Temp_Max_Count + 1
                if Temp_Max_Count == 1:
                    Final_dic["Temp_Max_data"] = items[i]

                elif Temp_Max_Count == config["Rule_2"]["trigger_count"]:
                    Final_dic["Temp_Max_data"]["Rule"] = config["Rule_2"]["Rule_num"]
                    Final_dic["Temp_Max_data"]["Alert"] = config["Rule_2"]["max_alert"]

                    # Anomaly data will be sent to Database.py to upload it to dynamoDB
                    Database.Database.upload_anomaly_data(Final_dic["Temp_Max_data"])
                    data = Final_dic["Temp_Max_data"]
                    print('Alert for device_id', data['deviceid'], 'on rule', data['Rule'], 'starting at',
                          data['timestamp'], 'with the alert type', data['Alert'])
                    Temp_Max_Count = 0
                    Final_dic.pop("Temp_Max_data")


            elif items[i]['Average'] < config['Rule_2']['avg_min']:
                Temp_Max_Count = 0
                Temp_Min_Count = Temp_Min_Count + 1
                if Temp_Min_Count == 1:
                    Final_dic["Temp_Min_data"] = items[i]
                elif Temp_Min_Count == config["Rule_2"]["trigger_count"]:
                    Final_dic["Temp_Min_data"]["Rule"] = config["Rule_2"]["Rule_num"]
                    Final_dic["Temp_Min_data"]["Alert"] = config["Rule_2"]["min_alert"]

                    # Anomaly data will be sent to Database.py to upload it to dynamoDB
                    Database.Database.upload_anomaly_data(Final_dic["Temp_Min_data"])
                    data = Final_dic["Temp_Min_data"]
                    print('Alert for device_id', data['deviceid'], 'on rule', data['Rule'], 'starting at',
                          data['timestamp'], 'with the alert type', data['Alert'])
                    Temp_Min_Count = 0
                    Final_dic.pop("Temp_Min_data")

            #Making all counter 0 if the reading is within the limit
            else:
                Temp_Max_Count = 0
                Temp_Min_Count = 0
