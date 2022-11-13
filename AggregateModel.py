import datetime
import time
from Database import Database

# Get the service resource.
import Database

#This Function will aggregate the Raw_data.
#The aggregated data will be send to Database.py to upload it to dynamoDB.
def aggregate(table_name, dev_id):
    # Getting the raw data from Database.py
    items = Database.Database.get_data(table_name, dev_id)
    First_timestamp = datetime.datetime.strptime(items[0]['timestamp'], '%Y-%m-%d %H:%M:%S.%f')

    # creating the empty lists
    Temp_Val_list = []
    SPO2_Val_list = []
    Heart_Val_list = []
    Final_list = []

    #Creating all counters
    Minute_count = 0
    Temp_count = 0
    SPO2_count = 0
    Heart_count = 0

    #For loop for the Rawdata, which is in a list.
    for i in range(0, len(items)):
        Start_timestamp = First_timestamp + datetime.timedelta(minutes=Minute_count)
        End_timestamp = Start_timestamp + datetime.timedelta(minutes=1)
        Current_timestamp = datetime.datetime.strptime(items[i]['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
        if Current_timestamp >= End_timestamp:
            Avg_Temp_Val = sum(Temp_Val_list) / len(Temp_Val_list)
            Avg_SPO2_Val = sum(SPO2_Val_list) / len(SPO2_Val_list)
            Avg_Heart_Val = sum(Heart_Val_list) / len(Heart_Val_list)

            for j in range(0, len(Final_list)):
                if Final_list[j]['datatype'] == 'Temperature':
                    Final_list[j]['value'] = Avg_Temp_Val
                    Final_list[j]['min'] = min(Temp_Val_list)
                    Final_list[j]['max'] = max(Temp_Val_list)
                elif Final_list[j]['datatype'] == 'SPO2':
                    Final_list[j]['value'] = Avg_SPO2_Val
                    Final_list[j]['min'] = min(SPO2_Val_list)
                    Final_list[j]['max'] = max(SPO2_Val_list)
                elif Final_list[j]['datatype'] == 'HeartRate':
                    Final_list[j]['value'] = Avg_Heart_Val
                    Final_list[j]['min'] = min(Heart_Val_list)
                    Final_list[j]['max'] = max(Heart_Val_list)

            #For every minute data will be sent to Database.py to upload it to dynamoDB
            Database.Database.upload_agg_data(Final_list)

            Minute_count = Minute_count + 1
            Start_timestamp = First_timestamp + datetime.timedelta(minutes=Minute_count)
            End_timestamp = Start_timestamp + datetime.timedelta(minutes=1)

            #Resetting all counters and emptying all lists, once after the past one  data is sent to database.py
            Temp_Val_list = []
            SPO2_Val_list = []
            Heart_Val_list = []
            Final_list = []
            Temp_count = 0
            SPO2_count = 0
            Heart_count = 0


        #Data Aggregation for next one minute will be done below
        if End_timestamp > Current_timestamp >= Start_timestamp and items[i][
            'datatype'] == 'Temperature':
            Temp_Val_list.append(items[i]['value'])
            Temp_count = Temp_count + 1
            if Temp_count == 1:
                Final_list.append(items[i])


        if End_timestamp > Current_timestamp >= Start_timestamp and items[i][
            'datatype'] == 'SPO2':
            SPO2_Val_list.append(items[i]['value'])
            SPO2_count = SPO2_count + 1
            if SPO2_count == 1:
                Final_list.append(items[i])

        if End_timestamp > Current_timestamp >= Start_timestamp and items[i][
            'datatype'] == 'HeartRate':
            Heart_Val_list.append(items[i]['value'])
            Heart_count = Heart_count + 1
            if Heart_count == 1:
                Final_list.append(items[i])
