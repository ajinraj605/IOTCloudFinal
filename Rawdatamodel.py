import datetime

from database import Database


class RawDataModel:
    Rawdatamodel = 'raw_data'


    def _init_(self):
        self._db = Database()
        self._latest_error = ''

    #Latest error is used to store the error string in case an issue. It's reset at the beginning of a new function call
    @property
    def latest_error(self):
        return self._latest_error

    def new_table(self, name):
        # Create the DynamoDB table.
        created_table = self._db.create_table(name)
        print(f"Table has been created - {created_table}")

    def generate_agg(self, start_time, end_time):
        device_id = ['BSM_G101', 'BSM_G102']
        list1 =[]
        for i in range(0, len(device_id)):
            data_item = self._db.get_device_data(RawDataModel, device_id[i], start_time, end_time)

            for j in data_item:
                current_time = datetime.datetime.strptime(j['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
                if start_time < current_time < end_time:
                    if j['deviceid'] == device_id[i] and j['datatype'] == 'SPO2':
                        list1.append(j)
        for j in list1:
            print(j['deviceid'], ":", j['datatype'], ":", j['timestamp'], ":", j['value'])