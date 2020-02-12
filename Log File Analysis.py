import boto3

import numpy as np
import pandas as pd
import datetime 

s3 = boto3.resource('s3',aws_access_key_id='<ACCESS_KEY>',aws_secret_access_key='<SECRET_KEY>')

bucket = s3.Bucket('<BUCKET_NAME>')

# Create DataFrame
df = pd.DataFrame()

#Initalizing 
index = 0
date_format='%Y-%m-%dT%X'

for obj in bucket.objects.filter(Prefix='PCC/PROD_Logs/AUTOMATIC/'):
  key = obj.key
  key=str(key)
  body = obj.get()['Body'].read()
  body=body.decode("utf-8")
  list_of_lines=body.splitlines()

  for line in list_of_lines:
    if "Parsed PCC Token Code" in line:
      time=line.partition(' ')[0]
      time = time[:-6]
      datetime_object=datetime.datetime.strptime(time, date_format)
      df.loc[index,'Start Time']=datetime_object
      df.loc[index,'Log File Name']=key
    elif "EXIT APP\" message=\"Successfully Logged out from RC 2.0" in line:
      df=df.append({'End Time':"null"}, ignore_index=True)
      df.loc[index,'End Time']="null"
      index=index+1
      df.loc[index,'Log File Name']=key
      continue
    elif "Navigating to Patients Dashboard Screen" in line:
      time=line.partition(' ')[0]
      time = time[:-6]
      datetime_object=datetime.datetime.strptime(time, date_format)
      df.loc[index,'End Time']=datetime_object
      df.loc[index,'Log File Name']=key
      index=index+1


indexNames = df[df['End Time'] == 'null'].index
df.drop(indexNames , inplace=True)


df['End Time']=df['End Time'].values.astype('datetime64[ns]')

df['Duration']=df['End Time']-df['Start Time']

df['duration_in_sec']=df['Duration'] / np.timedelta64(1, 's')

df.to_csv('Output Data.csv', index=False, encoding='utf-8')

#Dropping cases 566 and 353 - This occured due to Sync button case
df = df[df['duration_in_sec'] < 200] 


#df.to_csv('Output Data.csv', index=False, encoding='utf-8')



print('Maximum Time for login is '+ str(max(df['duration_in_sec'])) + 's')
print('Minimum Time for login is '+ str(min(df['duration_in_sec'])) + 's')
print('Average Time for login is '+ str((df['duration_in_sec']).mean()) +'s')