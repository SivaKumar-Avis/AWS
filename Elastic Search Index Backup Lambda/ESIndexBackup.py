__author__ = '621992'

import http.client
import datetime
import json
import os
import dateutil.tz
import boto3
from botocore.exceptions import ClientError

index_url=os.environ['INDEX_URL']


def find_index_to_be_backedup():
    time_zone=os.environ['TIME_ZONE']
    backup_day=os.environ['BACKUP_DAYS']
    backup_day_int=int(backup_day)
    est = dateutil.tz.gettz(time_zone)
    delete_date = datetime.datetime.now(tz=est) + datetime.timedelta(backup_day_int)
    del_date=delete_date.strftime("%Y%m%d")
    print(del_date)

    index_prefix=os.environ['INDEX_PREFIX']
    index=index_prefix+del_date
    print("Index to be backed up is "+index)
    return index


def check_snapshot(index_to_be_bacup):
    conn = http.client.HTTPSConnection(index_url)

    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache",
        'postman-token': "b238578a-6025-3194-0ceb-0f969690248e"
        }

    conn.request("GET", "/_snapshot/es-backup-repo/"+str(index_to_be_bacup)+"/", headers=headers)

    res = conn.getresponse()
    data = res.read()
    response=data.decode("utf-8")
    return res,response


def move_index(index1):
    conn = http.client.HTTPSConnection(index_url)

    index_temp= "\"" + str(index1) +"\""

    payload = "{\r\n  \"indices\":"+str(index_temp)+",\r\n  \"ignore_unavailable\": true,\r\n  \"include_global_state\": false\r\n}\r\n"

    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache",
        'postman-token': "19aeec34-3b9b-af5a-a060-2a77b74532b6"
     }

    conn.request("PUT", "/_snapshot/es-backup-repo/"+str(index1), payload, headers)

    res = conn.getresponse()
    data = res.read()
    response=data.decode("utf-8")
    print(response)


def get_snapshot_detail(index2):
    conn = http.client.HTTPSConnection(index_url)

    headers = {
         'content-type': "application/json",
         'cache-control': "no-cache",
         'postman-token': "2622e19d-8008-67aa-eee1-a304b92ba2cd"
        }

    conn.request("GET", "/_snapshot/es-backup-repo/"+str(index2), headers=headers)

    res = conn.getresponse()
    data = res.read()

    resp_temp=str(data.decode("utf-8"))

    x=json.loads(resp_temp)
    status=str(x["snapshots"][0]["state"])
    print(status)
    return status


def is_index_exists(index_to_be_bacup):
    conn = http.client.HTTPSConnection(index_url)

    headers = {
        'cache-control': "no-cache",
        'postman-token': "a0a65101-21e0-f7d1-c21b-3879b8a19620"
     }

    index_temp= "/" + index_to_be_bacup
    conn.request("GET",str(index_temp),headers=headers)


    res = conn.getresponse()
    data = res.read()

    if(res.status==404):
        return "False"
    else:
        return "True"


def delete_index(indx):
    conn = http.client.HTTPSConnection(index_url)

    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache",
        'postman-token': "c2576aa8-b50c-9d41-5dcd-59b96164c9be"
        }
    index_temp="/"+str(indx)
    conn.request("DELETE",str(index_temp), headers=headers)

    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))


def delete_snapshot(index):
    conn = http.client.HTTPSConnection(index_url)

    headers = {
        'cache-control': "no-cache",
        'postman-token': "a71a1421-90d3-c131-ae6c-c1b5b3fd5e9d"
        }

    conn.request("DELETE", "/_snapshot/es-backup-repo/"+str(index), headers=headers)

    res = conn.getresponse()
    data = res.read()

    print(data.decode("utf-8"))


def send_mail():
    sender=os.environ['SENDER']
    recipient=os.environ['RECIPIENT']
    aws_region=os.environ['REGION']
    subject=os.environ['SUBJECT']
    charset = "UTF-8"
    body_text=("Team,\r\n"
               "     Elasticsearch index snapshot has been failed "
              )

    client = boto3.client('ses',region_name=aws_region)

    try:
        response = client.send_email(
        Destination={
            'ToAddresses': [
                recipient,
            ],
        },
        Message={
            'Body': {
                'Text': {
                    'Charset': charset,
                    'Data': body_text,
                },
            },
            'Subject': {
                'Charset': charset,
                'Data': subject,
            },
        },
        Source=sender
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email has been sent to RTB regarding failure ")



def lambda_handler(event, context):

    index_to_be_bacup=find_index_to_be_backedup()
    res,response=check_snapshot(index_to_be_bacup)
    if res.status==404:
        print("No Snapshot is created yet")
        index_check=is_index_exists(index_to_be_bacup)
        if(index_check.lower()=="True".lower()):
            move_index(index_to_be_bacup)
            print("Taking Snapshot")
        else:
            print("Index itself not exists")
    elif res.status!=404:
        status=get_snapshot_detail(index_to_be_bacup)
        print("getting snapshot detail")
        if status.lower()=="SUCCESS".lower():
            index_check=is_index_exists(index_to_be_bacup)
            if(index_check.lower()=="True".lower()):
                print("Deleting Index")
                delete_index(index_to_be_bacup)
            else:
                print("Index Already Deleted")
        elif status.lower()=="INPROGRESS".lower():
            print("Snapshot is still running")
        elif status.lower()=="FAILED".lower():
            est = dateutil.tz.gettz(os.environ['TIME_ZONE'])
            sys_time = datetime.datetime.now(tz=est)
            sys_time_format=sys_time.strftime("%H:%M")
            begin_time=os.environ['BEGIN_TIME']
            end_time=os.environ['END_TIME']
            if(sys_time_format > begin_time and sys_time_format < end_time):
                send_mail()
                print("Send mail to RTB")
            else:
                delete_snapshot(index_to_be_bacup)
                move_index(index_to_be_bacup)
                print("Snapshotting has been retried again for the 2nd period from FAILED status")




