from Database import Database
import datetime
import AlertDataModel
import AggregateModel

# Create the aggregate table
name = 'Agg_Table'
Database.create_table(name)

# Create the anomaly table
name = 'Anomaly_Table'
Database.create_table(name)


print('Aggregating data for device BSM_G101')
# Generate the aggregate data
table_name = 'ProjectRawData'
dev_id = 'BSM_G101'
AggregateModel.aggregate(table_name, dev_id)

print('Aggregating data for device BSM_G102')
# Generate the aggregate data
dev_id = 'BSM_G102'
AggregateModel.aggregate(table_name, dev_id)


print('Processing rules for device BSM_G101')
#Generate the anomaly data
dev_id = 'BSM_G101'
AlertDataModel.anomaly_detector(dev_id)

print('Processing rules for device BSM_G102')
#Generate the anomaly data
dev_id = 'BSM_G102'
AlertDataModel.anomaly_detector(dev_id)
