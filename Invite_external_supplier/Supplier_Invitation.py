import requests
import json
import pandas as pd
import cx_Oracle
import boto3
import base64
from botocore.exceptions import ClientError
from boto3.s3.transfer import S3Transfer
from datetime import datetime, timedelta,date
import msal
import re
import logging
import sys
from logging.config import fileConfig
import csv
import sys

sys.path.append("/home/ec2-user/CREDENTIAL/ALL_CREDENTIALS")
import credentials as cdtls


sys.path.append("/home/ec2-user/ERROR_LOGGER/ETL_LOGGER")
from logger import logger



Logger = logger()
Logger.begin(process_name = 'External Supplier Invitation')

date_today = date.today().strftime("%Y-%m-%d")

print('#########################################################################')
print('#################### External Supplier Invitation #######################')
print('Runnnig for date ', date_today)



def credential(id,key):
        secret_name = "arn:aws:secretsmanager:us-east-1:103704162378:secret:SupplierAzureAccess-FD8wzx"
        region_name = "us-east-1"
            # Create a Secrets Manager client
        session = boto3.session.Session(aws_access_key_id=id,aws_secret_access_key=key)
        client = session.client(
                service_name='secretsmanager',
                region_name=region_name
            )
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        tenant_id = (eval(get_secret_value_response['SecretString']))['tenant_id']
        client_id = (eval(get_secret_value_response['SecretString']))['client_id']
        client_secret = (eval(get_secret_value_response['SecretString']))['client_secret']
        group_id = (eval(get_secret_value_response['SecretString']))['group_id']
        return tenant_id, client_id, client_secret,group_id



dsn_tns = tns_dsn = cx_Oracle.makedsn(cdtls.oracle_bidwprd.host, cdtls.oracle_bidwprd.port, service_name=cdtls.oracle_bidwprd.service_name) 
connection = cx_Oracle.connect(user=cdtls.oracle_bidwprd.user, password=cdtls.oracle_bidwprd.password, dsn=tns_dsn)

df_ora = pd.read_sql("select distinct email_address from bi_dw.wc_sc_user_d where POWER_BI_ACCESS = 1 and vendor_type='DIRECT MATERIAL'", con=connection)
connection.close()

tenant_id, client_id, client_secret,group_id = credential(id = cdtls.aws_s3.aws_access_key_id, key = cdtls.aws_s3.aws_secret_access_key)


def get_access_token(client_id,client_secret,tenant_id):
    print('access token getting started')
    # Initialize MSAL client application
    app = msal.ConfidentialClientApplication(
        client_id         = client_id,
        client_credential = client_secret,
        authority         = f"https://login.microsoftonline.com/{tenant_id}")

    SCOPE = ['https://graph.microsoft.com/.default']
    # Get an access token for the Graph API using client credentials
    result = app.acquire_token_silent(SCOPE, account=None)
    if not result:
        result = app.acquire_token_for_client(scopes=SCOPE)

    access_token = result['access_token']
    print('access token getting complete')
    return access_token


def get_all_members(group_id,access_token):
    print(" Fetching All Members Data from Azure AD Group")
    url = f"https://graph.microsoft.com/v1.0/groups/{group_id}/members?$select=displayName,mail,otherMails"

    # Initialize an empty list to store the members
    members = []

    # Loop through the pages of results until all members are retrieved
    while True:
        response = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
        response_data = response.json()

        # Add the members from the current page to the list
        members.extend(response_data["value"])

        # Check if there are more pages of results
        if "@odata.nextLink" not in response_data:
            break
        # Update the URL to get the next page of results
        url = response_data["@odata.nextLink"]

    df_ad = pd.DataFrame(members)
    print(" Fetching All Members Data from Azure AD Group Complete")
    return df_ad


def bulk_invite_user(df_diff,access_token):
    print('bulk invite started')
    error_user = []
    api_url = f'https://graph.microsoft.com/v1.0/invitations'
    with open('error_suppliers_list.csv', 'a', newline="") as f:
        writer = csv.writer(f)
        for index,row in df_diff.iterrows():
            print('invite user is started for: '+row['DisplayName'])
            # local_logger.info("Starting invite for ")
        # Define the JSON payload for the invite request
            invite_payload = {
                'invitedUserDisplayName': row['DisplayName'],
                'invitedUserEmailAddress': row['email'],
                'inviteRedirectUrl': "https://login.microsoftonline.com/",
                'invitedUserMessageInfo': {
                    'customizedMessageBody': 'Welcome to AAM External Supplier Scorecard',
                    'messageLanguage': 'en-US'
                },
                "sendInvitationMessage":True
            }

            # Make a POST request to the Graph API with the access token and JSON payload
            headers = {
                'Authorization': 'Bearer ' + access_token,
                'Content-Type': 'application/json'
            }
            response = requests.post(api_url, headers=headers, data=json.dumps(invite_payload))
            if response.status_code == 201:
                # The request was successful
                print('invite complete for: '+row['email'])
            else:
                print('invite for user: '+row['email']+' failed')
                error_user.append(row['email'])
                ll = [row['email'],row['DisplayName']]
                writer.writerow(ll)
                # The request failed
        f.close()
    print('bulk invite complete')
    return error_user


def get_user_diff(df_ad,df_ora):
    df_ext = df_ad[~df_ad['mail'].str.contains("@aam.com")]
    df_ext = df_ext[~df_ext['mail'].str.contains("@AAM.COM")]
    df_ora = df_ora[df_ora['EMAIL_ADDRESS'].str.contains("@")]
    
    l_ad = (list(set([a for b in df_ext['otherMails'].tolist() for a in b])))

    # l_ad = list(df_ext.mail.unique())

    l_ad = [x.upper() for x in l_ad]

    l_ad = list(set(l_ad))
    l_ad = [s.replace('\r', '') for s in l_ad]
    l_ad = [s.replace('\t', '') for s in l_ad]

    l_ora = list(df_ora.EMAIL_ADDRESS.unique())
    l_ora = [x.upper() for x in l_ora]
    l_ora = list(set(l_ora))

    erro_df = pd.read_csv('/home/ec2-user/Supplier_ScoreCard/error_suppliers_list.csv')
    erro_df_email = list(erro_df.email.unique())
    l_ora = [i for i in l_ora if i not in erro_df_email]
    l_ora = [s.replace('\r', '') for s in l_ora]
    l_ora = [s.replace('\t', '') for s in l_ora]

    
    
    l_diff = [x for x in l_ora if x not in l_ad]

    df_diff = pd.DataFrame({'email':l_diff})
    print('df_diff shape : ',df_diff.shape)
    return df_diff

access_token = get_access_token(client_id,client_secret,tenant_id)
df_ad = get_all_members(group_id,access_token)
df_diff = get_user_diff(df_ad,df_ora)

df_diff['DisplayName'] = [x.split("@")[0] for x in df_diff['email']]
df_diff['DisplayName'] = [re.sub("[.]", " ", x) for x in df_diff['DisplayName']]

error_user = bulk_invite_user(df_diff,access_token)
print('length error email',len(error_user))

if len(error_user) >0:
    try:
        Logger.start(log_info = 'external supplier invitation')
        print('try to read unknown csv file')
        ll = pd.read_csv('abc.csv')
        print(ll.shape)
    ####################################################################################################
    except Exception as e:
        Logger.update_error(error="external supplier invitation fail for usres : "+str(error_user))
    finally:
        Logger.end(email_alert=True, email_to=['1486ed74.aam.onmicrosoft.com@amer.teams.ms'])
    ####################################################################################################
else:
    pass
    print('invite users is complete')