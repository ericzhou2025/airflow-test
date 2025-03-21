# from oneapi_common.utils import get_logger
import pandas as pd
from datetime import datetime
import logging
import urllib3
import os
import numpy as np
import msal
import requests
from . import DICT_CLIENT,DICT_TEAMS_FILE,GRAPH_API_BASIC_URL
from airflow.logging_config import log as logger


def get_logger() -> logging.Logger:
    """Creates logger with right log level(DEBUG or INFO).

    For logs which might contain sensitive client
    information, let's use `logger.debug()`

    Debug logs will NOT be displayed in AWS.
    Only INFO and greater logs(WARNING, ERROR, CRITICAL) will be
    displayed on AWS.

    returns: logger object
    """

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "DEBUG"))
    logger = logging.getLogger(__name__)

    return logger

def clean_df(df):
    # clean_df = df.where((pd.notnull(df)), None)
    clean_df = df.replace({np.nan:None})
    for col in clean_df.columns.values:
        if col in ['created_at', 'updated_at', \
                    'my_investor_created_at','my_investor_updated_at']:
            clean_df[col] = pd.to_datetime(df[col],  format="%Y-%m-%d %H:%M:%S").astype(str)
        elif col in ['_importedAt','my_investor__importedAt']:
            clean_df[col] = pd.to_datetime(df[col],  format="%Y-%m-%d %H:%M:%S.%f").astype(str)
    return clean_df

def cal_interval(gcp_db, gcp_table, date_field_name='updated_at'):
    try:
        sql = 'SELECT {0} FROM {1} order by {0} desc limit 1'.format(date_field_name, gcp_table)
        df = gcp_db.fetch_query_as_dataframe(sql)
        last_date = df.iloc[0][0].strftime("%Y-%m-%d %H:%M:%S")
        if last_date is not None:
            last_date = datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S")
            current_date = datetime.now()
            interval = current_date - last_date
            interval_days = interval.days + 1
            return interval_days
    except Exception as e:
        raise Exception('Get latest updated date failed in table {0}. Error {1}'.format(gcp_table,e))


def get_token():
    client_id = DICT_CLIENT['client_id']
    client_secret = DICT_CLIENT['client_secret']
    tenant_id = DICT_CLIENT['tenant_id']
    authority = 'https://login.microsoftonline.com/{0}'.format(tenant_id)
    scopes = ["https://graph.microsoft.com/.default"]

    app = msal.ConfidentialClientApplication(
                    client_id=client_id,
                    client_credential=client_secret,
                    authority=authority)

    try:
        result = app.acquire_token_for_client(scopes)
        if result is not None and "access_token" in result:
            return result["access_token"]
        else:
            return None
    except Exception as e:
        raise Exception('Get token faild. Error {0}'.format(e))

def get_graph_api_result(api_url, token):
    try:
        graph_data = requests.get(api_url,headers={'Authorization': 'Bearer ' + token})
        if 'error' in graph_data:
            token = get_token()
            graph_data = requests.get(api_url,headers={'Authorization': 'Bearer ' + token})

        return graph_data
    except Exception as e:
        logging.error("Unable to get result by url %s. Error: %s" % (api_url, e))
        return None


def download_sharepoint_file(table_name, token):
    site_name = DICT_TEAMS_FILE[table_name]['site_name']
    file_name = DICT_TEAMS_FILE[table_name]['file_name']

    site_id = get_id_by_site_name(site_name, token)
    item_id = get_id_by_item_name(site_id, file_name, token)

    api_url = '{0}/sites/{1}/drive/items/{2}/content'.format(GRAPH_API_BASIC_URL, site_id, item_id)
    logger.info(f'try to download {api_url}, write to {"{0}.xlsx".format(table_name)}')
    response = get_graph_api_result(api_url, token)
    if response:
        content = response.content
        with open('{0}.xlsx'.format(table_name),'wb') as f:
            f.write(content)
    else:
        logger.info(f'response.status_code = {response.status_code}')
        raise Exception('Unable to download content')

def get_id_by_site_name(site_name, token):
    api_url = '{0}/sites/mdigital.sharepoint.com:/sites/{1}?$select=id'.format(GRAPH_API_BASIC_URL, site_name)
    response = get_graph_api_result(api_url, token)
    if response:
        response_data = response.json()
        site_id = response_data.get('id', None)
        return site_id
    else:
        return None

def get_id_by_item_name(site_id, file_name, token):
    api_url = '{0}/sites/{1}/drive/root/search(q=\'{{2}}\')'.format(GRAPH_API_BASIC_URL, site_id, file_name)
    response = get_graph_api_result(api_url, token)
    if response:
        response_data = response.json()
        values = response_data.get('value', None)
        if values:
            item_id = None
            for v in values:
                if v['name'] == file_name:
                    item_id = v['id']
                    break
        return item_id
    else:
        return None

def parse_excel_file(type, infile_list):
    if type == 'ia_order':
        df = pd.read_excel(infile_list[0],skiprows=1, engine="xlrd")
        key_fields = ['Product Name', 'Quantity', 'Promise date to Global','Receive date from LNX','Comments','MFG plan','Note']

        data_list = []
        for i in range(len(df)):
            data_line_list = []
            data_line = df.iloc[i]
            if not isinstance(data_line[key_fields[0]],str):
                continue

            data_line_list.append(data_line[key_fields[0]].strip())
            data_line_list.append(data_line[key_fields[1]])
            if key_fields[2] not in df.columns.names or pd.isna(data_line[key_fields[2]]) or data_line[key_fields[2]] == 'TBD':
                data_line_list.append(None)
            else:
                data_line_list.append(str(data_line[key_fields[2]]))

            for field in key_fields[3:7]:
                if isinstance(data_line[field],float) :
                    data_line[field] = None
                else:
                    data_line[field] = str(data_line[field])
                data_line_list.append(data_line[field])

            data_list.append(data_line_list)
        new_df = pd.DataFrame(data_list,columns=['material_code', 'quantity', 'promise_date', 'receive_date_from_lnx', 'comments', 'mfg_plan', 'note'])
    elif type == 'ia_setup':
        df = pd.read_excel(infile_list[0],sheet_name='Data Entry Page',skiprows=2, engine="xlrd")
        stages = ['PE Ready', 'Set Up Initiated', 'Formulation Sent to Customer', 'Formulation Signed By Customer', \
            ' PE Request for Cost Roll', 'Cost Roll Complete', 'PE Request for MM', 'MM Completed', 'PE Request for SDS', 'SDS Completed']
        key_fields = ['Product Number', 'Product Name', 'Unnamed: 8', 'Date Request Submitted']

        for field in stages + key_fields:
            if field not in df.columns:
                print('cannot find column %s' % field)

        data_list = []
        data_dict = {}
        df = df.sort_values(key_fields[3], ascending=False)

        for i in range(len(df)):
            data_line = df.iloc[i]
            if not isinstance(data_line[key_fields[0]],str):
                continue
            for field in key_fields[1:3]:
                if not isinstance(data_line[field],str):
                    data_line[field] = None
            part_no = data_line[key_fields[0]].strip()
            if part_no in data_dict.keys():
                continue   ## only keep latest part no if duplicate
            data_dict[part_no] = 1
            data_line_list = []
            data_line_list.append(part_no)
            data_line_list.append(data_line[key_fields[1]])
            data_line_list.append(data_line[key_fields[2]])

            init_stage = 'not started'
            last_stage = init_stage
            for stage in stages:
                if pd.isna(data_line[stage]):
                    break
                else:
                    last_stage = stage
            data_line_list.append(last_stage)
            data_list.append(data_line_list)
        new_df = pd.DataFrame(data_list, columns=['part_no', 'product_name', 'setup_site', 'tracker_status'])
    elif type == 'gmp_setup':
        LNX_fields_list = ['Product number', 'Product Name', 'Status', 'Date created in SFDC']
        IRV_fields_list = ['Product Number', 'Product Name', 'Status', 'Date PM task assigned in SFDC']
        data_list1 = parse_gmp_excel(infile_list[0],"LNX NPI & SPEC CC's",1,LNX_fields_list,'LNX')
        data_list2 = parse_gmp_excel(infile_list[1],"Projects",3,IRV_fields_list,'IRV')
        data_list = data_list1 + data_list2
        new_df = pd.DataFrame(data_list, columns=['part_no', 'product_name', 'setup_site', 'request_date','tracker_status'])
        new_df.sort_values(by=['part_no','request_date'],ascending=False, inplace=True)
        new_df.drop_duplicates(subset=['part_no'],keep='first', inplace=True)
        new_df.drop(['request_date'],axis=1,inplace=True)
    return new_df

def parse_gmp_excel(excel_file, sheet_name, skip_row_no, fields_list, site_name):
    df = pd.read_excel(excel_file,sheet_name=sheet_name, skiprows=skip_row_no, engine="xlrd")
    key_fields = fields_list

    data_list = []
    for i in range(len(df)):
        data_dict = {}
        data_line = df.iloc[i]
        if not isinstance(data_line[key_fields[0]],str):
            continue
        for field in key_fields[1:3]:
            if not isinstance(data_line[field],str):
                data_line[field] = None

        if data_line[key_fields[0]] in ['tbc','see column H','TBD', 'TBC', 'test']:
            continue
        data_dict['part_no'] = data_line[key_fields[0]].strip()
        data_dict['product_name']= data_line[key_fields[1]]
        data_dict['setup_site'] = site_name
        data_dict['tracker_status'] = data_line[key_fields[2]]
        data_dict['request_date'] = data_line[key_fields[3]]
        data_list.append(data_dict)
    return data_list
