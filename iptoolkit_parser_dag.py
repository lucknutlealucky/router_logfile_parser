from datetime import datetime
from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.base import BaseHook
import ftplib
import glob
import os
import pathlib
import pandas as pd
import pendulum
import shutil

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator

from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import DagRun, DagModel
from airflow.utils.state import State
from airflow import settings

import py7zr
import io
import pandas as pd
import numpy as np
import time

from parser_config.huawei_mac_parser import parse_mac_info as hw_mac_parser
from parser_config.huawei_arp_parser import get_dataframe_huawei_arp as hw_arp_info_parser
from parser_config.huawei_iface_util import hw_get_iface_info as hw_get_iface_util_brif
from parser_config.huawei_iface_util import hw_get_iface_info_main as hw_get_iface_util_brif_main
from parser_config.huawei_bgp_ospf import get_hw_bgp as hw_bgp_parser
from parser_config.huawei_disp_iface import get_hw_iface as hw_disp_iface_all
from parser_config.huawei_disp_lldp_neighbor import hw_disp_lldp_neighbor as hw_lldp_neighbor
from parser_config.huawei_ipv6_neighbors import hw_ipv6_neighbors as hw_ipv6_neighbors
from parser_config.huawei_eth_trunk import hw_disp_eth_trunk as hw_disp_eth_trunk
from parser_config.huawei_health import hw_disp_health as hw_disp_health
from parser_config.huawei_disp_devices import hw_disp_device as hw_disp_device
from parser_config.huawei_curr_conf_iface_incld_iface_ld_blnc import huawei_curr_conf_iface_incld_iface_ld_blnc_proc as huawei_curr_conf_iface_incld_iface_ld_blnc_proc

from parser_config.nokia_sh_router_arp import nk_sh_router_arp as nk_sh_router_arp
from parser_config.nokia_sh_srv_fdb_mac import nk_sh_srv_fdb_mac as nk_sh_srv_fdb_mac
from parser_config.nokia_sh_srv_id_arp import nk_sh_srv_id_arp as nk_sh_srv_id_arp
from parser_config.nokia_sh_router_iface import nk_sh_router_iface as nk_sh_router_iface


from parser_config.cisco_sh_arp_vrf_all import csc_sh_arp_vrf_all as csc_sh_arp_vrf_all
from parser_config.cisco_sh_l2vpn_fwrd_brdg_mac_loc import l2vpn_fwd_brdg_mac_loc as l2vpn_fwd_brdg_mac_loc
from parser_config.cisco_sh_interfaces import csc_sh_interfaces as csc_sh_interfaces
from parser_config.cisco_sh_lldp_neighbrs_detail import cisco_sh_lldp_neighbrs_detail as csc_sh_lldp_neighbrs_detail

from parser_config.zte_sh_arp import zte_sh_arp as zte_sh_arp
from parser_config.zte_sh_iface_brief import zte_sh_iface_brief as zte_sh_iface_brief
from parser_config.zte_sh_mac_l2vpn import zte_sh_mac_l2vpn as zte_sh_mac_l2vpn 
from parser_config.zte_sh_lldp_entry import zte_sh_lldp_entry as zte_sh_lldp_entry


def nokia_process(task_label, **kwargs):
    processed_data = kwargs['ti'].xcom_pull(task_ids='File_Mapper', key='processed_data')
    data = processed_data['file_name'].split('.')[0]
    file_dir = processed_data['file_dir']

    file_counter = 0
    start_time = time.time()

    nic_data = get_files(file_dir, data + '.nic')
    file_map = get_files(file_dir, data + '.csv').decode('utf-8')

    file_mapper = pd.read_csv(io.StringIO(file_map))
    file_mapper = file_mapper[file_mapper['vendor'] == task_label]

    file_mapper = file_mapper[['file_name']].dropna(ignore_index=True)
    file_list = file_mapper['file_name'].unique()
    listFile = list(file_list)

    print(f"Processing {len(listFile)} for {task_label}")

    #Put the Array Buffer Here

    show_router_arp = []
    show_service_fdb_mac = []
    show_service_id__arp = []
    show_router_interface = []

    ##

    print('Load the file....')
    with py7zr.SevenZipFile(io.BytesIO(nic_data), mode="r") as archive:
        print('File Loaded....')
        for f, d in archive.read(listFile).items():
            file_counter = file_counter + 1
            bytesData = d.read()
            string_data = bytesData.decode('utf-8')
            text_io = io.StringIO(string_data, newline=None)
            lines = text_io.readlines() 

            print_file_counter_status(file_counter, start_time)

            #Call The Parser Here And Apped to The Array Buffer
            if(data == 'Collection_IP_NOKIA_ZTE_CISCO_ARP'):
                show_router_arp.append(nk_sh_router_arp(lines,f))
                show_service_fdb_mac.append(nk_sh_srv_fdb_mac(lines,f))
                show_service_id__arp.append(nk_sh_srv_id_arp(lines,f))

            if(data == 'Collection_IP_NOKIA_ZTE_CISCO_BGP'):
                show_router_interface.append(nk_sh_router_iface(lines,f))


            # df = nk_sh_router_arp
    print("Collecting Result....")
    if(data == 'Collection_IP_NOKIA_ZTE_CISCO_ARP'):
        show_router_arp_df = pd.concat(show_router_arp, ignore_index=True)
        show_service_fdb_mac_df = pd.concat(show_service_fdb_mac, ignore_index=True)
        show_service_id__arp_df = pd.concat(show_service_id__arp, ignore_index=True)

    if(data == 'Collection_IP_NOKIA_ZTE_CISCO_BGP'):
        show_router_interface_df = pd.concat(show_router_interface, ignore_index=True)



    print("Saving Result.....")
    if(data == 'Collection_IP_NOKIA_ZTE_CISCO_ARP'):
        save_dataframe_files(file_dir, show_router_arp_df, 'nk_router_arp.csv')
        save_dataframe_files(file_dir, show_service_fdb_mac_df, 'nk_service_fdb_mac.csv')
        save_dataframe_files(file_dir, show_service_id__arp_df, 'nk_service_id__arp.csv')

    if(data == 'Collection_IP_NOKIA_ZTE_CISCO_BGP'):
        save_dataframe_files(file_dir, show_router_interface_df, 'nk_router_interface_df.csv')


def cisco_process(task_label, **kwargs):
    processed_data = kwargs['ti'].xcom_pull(task_ids='File_Mapper', key='processed_data')
    data = processed_data['file_name'].split('.')[0]
    file_dir = processed_data['file_dir']

    file_counter = 0
    start_time = time.time()

    nic_data = get_files(file_dir, data + '.nic')
    file_map = get_files(file_dir, data + '.csv').decode('utf-8')

    file_mapper = pd.read_csv(io.StringIO(file_map))
    file_mapper = file_mapper[file_mapper['vendor'] == task_label]

    file_mapper = file_mapper[['file_name']].dropna(ignore_index=True)
    file_list = file_mapper['file_name'].unique()
    listFile = list(file_list)

    print(f"Processing {len(listFile)} for {task_label}")

    #Put the Array Buffer Here
    show_arp_vrf_all = []
    show_l2vpn_forwarding_bridge_domain_mac_address_location = []
    show_interfaces = []
    show_lldp_neighbors_detail = []

    ##

    with py7zr.SevenZipFile(io.BytesIO(nic_data), mode="r") as archive:
        for f, d in archive.read(listFile).items():
            file_counter = file_counter + 1
            bytesData = d.read()
            string_data = bytesData.decode('utf-8')
            text_io = io.StringIO(string_data, newline=None)
            lines = text_io.readlines() 

            print_file_counter_status(file_counter, start_time)

            #Call The Parser Here And Apped to The Array Buffer
            if(data == 'Collection_IP_NOKIA_ZTE_CISCO_ARP'):
                show_arp_vrf_all.append(csc_sh_arp_vrf_all(lines,f))
                show_l2vpn_forwarding_bridge_domain_mac_address_location.append(l2vpn_fwd_brdg_mac_loc(lines,f))

            if(data == 'Collection_IP_NOKIA_ZTE_CISCO_BGP'):
                show_interfaces.append(csc_sh_interfaces(lines,f))
                show_lldp_neighbors_detail.append(csc_sh_lldp_neighbrs_detail(lines,f))

            # df = hw.parse_mac_info(text_io)

    print("Collecting Result....")
    if(data == 'Collection_IP_NOKIA_ZTE_CISCO_ARP'):
        show_arp_vrf_all_df = pd.concat(show_arp_vrf_all, ignore_index=True)
        show_l2vpn_forwarding_bridge_domain_mac_address_location_df = pd.concat(show_l2vpn_forwarding_bridge_domain_mac_address_location, ignore_index=True)

    if(data == 'Collection_IP_NOKIA_ZTE_CISCO_BGP'):
        show_interfaces_df = pd.concat(show_interfaces, ignore_index=True)
        show_lldp_neighbors_detail_df = pd.concat(show_lldp_neighbors_detail, ignore_index=True)



    print("Saving Result.....")
    if(data == 'Collection_IP_NOKIA_ZTE_CISCO_ARP'):
        save_dataframe_files(file_dir, show_arp_vrf_all_df, 'csc_arp_vrf_all.csv')
        save_dataframe_files(file_dir, show_l2vpn_forwarding_bridge_domain_mac_address_location_df, 'csc_l2vpn_forwarding_bridge_domain_mac_address_location_df.csv')

    if(data == 'Collection_IP_NOKIA_ZTE_CISCO_BGP'):
        save_dataframe_files(file_dir, show_interfaces_df, 'csc_interfaces.csv')
        save_dataframe_files(file_dir, show_lldp_neighbors_detail_df, 'csc_lldp_neighbors_detail.csv')
        




def zte_process(task_label, **kwargs):
    processed_data = kwargs['ti'].xcom_pull(task_ids='File_Mapper', key='processed_data')
    data = processed_data['file_name'].split('.')[0]
    file_dir = processed_data['file_dir']

    file_counter = 0
    start_time = time.time()

    nic_data = get_files(file_dir, data + '.nic')
    file_map = get_files(file_dir, data + '.csv').decode('utf-8')

    file_mapper = pd.read_csv(io.StringIO(file_map))
    file_mapper = file_mapper[file_mapper['vendor'] == task_label]

    file_mapper = file_mapper[['file_name']].dropna(ignore_index=True)
    file_list = file_mapper['file_name'].unique()
    listFile = list(file_list)

    print(f"Processing {len(listFile)} for {task_label}")

    #Put the Array Buffer Here
    show_arp = []
    show_interface_brief = []
    show_mac_l2vpn = []
    show_lldp_entry = []
    

    ##

    with py7zr.SevenZipFile(io.BytesIO(nic_data), mode="r") as archive:
        for f, d in archive.read(listFile).items():
            file_counter = file_counter + 1
            bytesData = d.read()
            string_data = bytesData.decode('utf-8')
            text_io = io.StringIO(string_data, newline=None)
            lines = text_io.readlines() 

            print_file_counter_status(file_counter, start_time)
            #Call The Parser Here And Apped to The Array Buffer

            if(data == 'Collection_IP_NOKIA_ZTE_CISCO_ARP'):
                show_arp.append(zte_sh_arp(lines,f))
                show_mac_l2vpn.append(zte_sh_mac_l2vpn(lines,f))

            elif(data == 'Collection_IP_NOKIA_ZTE_CISCO_BGP'):
                show_interface_brief.append(zte_sh_iface_brief(lines,f))
                show_lldp_entry.append(zte_sh_lldp_entry(lines,f))
                

            # df = hw.parse_mac_info(text_io)

    print("Collecting Result....")
    if(data == 'Collection_IP_NOKIA_ZTE_CISCO_ARP'):
        show_arp_df = pd.concat(show_arp, ignore_index=True)
        show_mac_l2vpn_df = pd.concat(show_mac_l2vpn, ignore_index=True)

    elif(data == 'Collection_IP_NOKIA_ZTE_CISCO_BGP'):
        show_interface_brief_df = pd.concat(show_interface_brief, ignore_index=True)
        show_lldp_entry_df = pd.concat(show_lldp_entry, ignore_index=True)
        


    print("Saving Result.....")
    if(data == 'Collection_IP_NOKIA_ZTE_CISCO_ARP'):
        save_dataframe_files(file_dir, show_arp_df, 'zte_arp.csv')
        save_dataframe_files(file_dir, show_mac_l2vpn_df, 'zte_show_mac_l2vpn.csv')

    elif(data == 'Collection_IP_NOKIA_ZTE_CISCO_BGP'):
        save_dataframe_files(file_dir, show_interface_brief_df, 'zte_show_interface_brief.csv')
        save_dataframe_files(file_dir, show_lldp_entry_df, 'show_lldp_entry.csv')
       

def huawei_process(task_label, **kwargs):
    processed_data = kwargs['ti'].xcom_pull(task_ids='File_Mapper', key='processed_data')
    data = processed_data['file_name'].split('.')[0] 
    file_dir = processed_data['file_dir']

    file_counter = 0
    start_time = time.time()

    nic_data = get_files(file_dir, data + '.nic')
    file_map = get_files(file_dir, data + '.csv').decode('utf-8')

    file_mapper = pd.read_csv(io.StringIO(file_map))
    file_mapper = file_mapper[file_mapper['vendor'] == task_label]


    file_mapper = file_mapper[['file_name']].dropna(ignore_index=True)
    file_list = file_mapper['file_name'].unique()
    listFile = list(file_list)

    print(f"Processing {len(listFile)} for {task_label}")

    #Put the Array Buffer Here
    display_arp_all = []
    display_mac_address = []
    display_interface_brief = []
    display_interface_brief_main = []
    display_bgp_vpnv4_all_peer = []
    display_interface = []
    display_interface_rsl = []
    display_lldp_neighbor = []
    display_ipv6_neighbors_brief = []
    display_eth_trunk = []
    display_health = []
    display_device = []
    display_current_configuration_interface_include_interface_load_balance = []
    ##

    limit = 0

    with py7zr.SevenZipFile(io.BytesIO(nic_data), mode="r") as archive:
        print('Starting Parsing Jobs.........')
        for f, d in archive.read(listFile).items():
            limit = limit + 1
            file_counter = file_counter + 1
            bytesData = d.read()
            string_data = bytesData.decode('utf-8')
            text_io = io.StringIO(string_data, newline=None)
            lines = text_io.readlines() 

            print_file_counter_status(file_counter, start_time)

            if(data == 'IP_Huawei_ARP'):
                display_current_configuration_interface_include_interface_load_balance.append(huawei_curr_conf_iface_incld_iface_ld_blnc_proc(lines,f))
                display_arp_all.append(hw_arp_info_parser(lines))
                display_device.append(hw_disp_device(lines,f))
                display_mac_address.append(hw_mac_parser(lines))

            elif(data == 'IP_Huawei_BGP'):
                #special
                buff = hw_disp_iface_all(lines,f)
                display_interface.append(buff[0])
                display_interface_rsl.append(buff[1])

                display_interface_brief.append(hw_get_iface_util_brif(lines))
                display_interface_brief_main.append(hw_get_iface_util_brif_main(lines))
                display_bgp_vpnv4_all_peer.append(hw_bgp_parser(lines))
                display_lldp_neighbor.append(hw_lldp_neighbor(lines,f))
                display_ipv6_neighbors_brief.append(hw_ipv6_neighbors(lines,f))
                display_eth_trunk.append(hw_disp_eth_trunk(lines,f))
                display_health.append(hw_disp_health(lines,f))

            # if limit == 50:
            #     break


    print("Collecting Result....")
    if(data == 'IP_Huawei_ARP'):
        display_arp_all_df = pd.concat(display_arp_all, ignore_index=True)
        display_current_configuration_interface_include_interface_load_balance_df = pd.concat(display_current_configuration_interface_include_interface_load_balance, ignore_index=True)
        display_mac_address_df = pd.concat(display_mac_address, ignore_index=True)
        display_device_df = pd.concat(display_device, ignore_index=True)
       

    elif(data == 'IP_Huawei_BGP'):
        display_health_df = pd.concat(display_health, ignore_index=True)
        display_interface_brief_df = pd.concat(display_interface_brief, ignore_index=True)
        display_interface_brief_main_df = pd.concat(display_interface_brief_main, ignore_index=True)
        display_interface_df = pd.concat(display_interface, ignore_index=True)
        display_interface_rsl_df = pd.concat(display_interface_rsl, ignore_index=True)
        display_lldp_neighbor_df = pd.concat(display_lldp_neighbor, ignore_index=True)
        display_ipv6_neighbors_brief_df = pd.concat(display_ipv6_neighbors_brief, ignore_index=True)
        display_eth_trunk_df = pd.concat(display_eth_trunk, ignore_index=True)
        display_bgp_vpnv4_all_peer_df = pd.concat(display_bgp_vpnv4_all_peer, ignore_index=True)


    print("Saving Result.....")
    if(data == 'IP_Huawei_ARP'):
        save_dataframe_files(file_dir, display_device_df, 'hw_devices.csv')
        save_dataframe_files(file_dir, display_current_configuration_interface_include_interface_load_balance_df, 'hw_current_configuration_interface_include_interface_load_balance.csv')
        save_dataframe_files(file_dir, display_arp_all_df, 'hw_arp_all.csv')
        save_dataframe_files(file_dir, display_mac_address_df, 'hw_mac_address.csv')
    
    elif(data == 'IP_Huawei_BGP'):
        save_dataframe_files(file_dir, display_interface_brief_df, 'hw_interface_brief.csv')
        save_dataframe_files(file_dir, display_interface_brief_main_df, 'hw_interface_brief_main.csv')
        save_dataframe_files(file_dir, display_bgp_vpnv4_all_peer_df, 'hw_bgp_vpnv4_all_peer.csv')
        save_dataframe_files(file_dir, display_interface_df, 'hw_interface.csv')
        save_dataframe_files(file_dir, display_interface_rsl_df, 'hw_interface_rsl.csv')
        save_dataframe_files(file_dir, display_lldp_neighbor_df, 'hw_lldp_neighbor.csv')
        save_dataframe_files(file_dir, display_ipv6_neighbors_brief_df, 'hw_ipv6_neighbors_brief.csv')
        save_dataframe_files(file_dir, display_eth_trunk_df, 'hw_eth_trunk_df.csv')
        save_dataframe_files(file_dir, display_health_df, 'hw_health.csv')
    


###############################################################################################
###############################################################################################
   
def print_file_counter_status(counter_status, start_time):
    elapsed_time = time.time() - start_time
    
    if counter_status % 1000 == 0:
        time_per_1000 = elapsed_time / (counter_status / 1000)
        print(f'{counter_status} mark reached, {time_per_1000:.2f} s/1000it')

def checkDataLog():
    mainsqlserver = MySqlHook("main_db")
    sql = '''SELECT id, filename FROM data_log.iptoolkit_parser_log where process_status = 'not_processed' LIMIT 1;'''
    df = mainsqlserver.get_pandas_df(sql=sql)
    df.columns = df.columns.str.lower()
    r = df.values.tolist()  # Convert DataFrame to 2D array (list of lists)
    return r

def insertDataLog(d, t, s):
    mainsqlserver = MySqlHook("main_db") 
    data = (d[0], d[1], d[2])

    insert_stmt = f"INSERT INTO {s}.{t} (filename, filesize, process_status) VALUES (%s, %s, %s)"
    
    connection = mainsqlserver.get_conn()
    cursor = connection.cursor()
    cursor.execute(insert_stmt, data)
    connection.commit()
    
    print(f'Inserted row: {data}')

def initial_process(**kwargs):
    current_date = pendulum.now("Asia/Jakarta").subtract(days=0).strftime("%Y%m%d")
    dataPath = BaseHook.get_connection("fs_IpToolkitData").description
    print('listing_files')
    list_of_files = (glob.glob(dataPath + '*.nic'))
    
    if len(list_of_files) > 0:
        print('got ' + str(len(list_of_files)) + 'files, starting joblisting')
        print(list_of_files)
        for l in list_of_files:
            if l.split('/')[-1] in ['Collection_IP_NOKIA_ZTE_CISCO_ARP.nic', 'IP_Huawei_BGP.nic', 'IP_Huawei_ARP.nic', 'Collection_IP_NOKIA_ZTE_CISCO_BGP.nic']:
                fs = os.path.getsize(l)
                fn = pathlib.Path(str(l)).stem.replace(' ','')
                last_modified_time = os.path.getmtime(l)
                ft = pendulum.from_timestamp(last_modified_time).to_datetime_string()
                fm = pendulum.from_timestamp(last_modified_time).strftime('%Y%m')

                target_directory = os.path.join(dataPath, "raw_data", fm)

                os.makedirs(target_directory, exist_ok=True)

                target_path = os.path.join(target_directory, os.path.basename(l).replace(' ',''))

                try:
                    shutil.copy2(l, target_path)

                    os.remove(l)

                    print(f"File copied and deleted successfully: {l}")
                    print(f"File size: {fs} bytes")
                    print(f"File name: {fn}")
                    print(f"Last modified: {ft}")
                    print(f"Target month {fm}")

                    dataLog = [fm + '||' + fn, fs, 'not_processed']
                    insertDataLog(dataLog,"iptoolkit_parser_log","data_log") 
                
                except Exception as e:
                    print(f"Failed to copy/delete file {l}: {e}")
        result = checkDataLog()
    else:
        print('no file candidate, cheking databse for jobs')
        result = checkDataLog()
    
    kwargs['ti'].xcom_push(key='initial_process_result', value=result)

def save_dataframe_files(file_dir, files_status_df, file_name):
    def write_initial_file_status(csv_buffer, csv_file_path):
        csv_buffer.seek(0)
        with open(csv_file_path, 'w') as local_file:
            local_file.write(csv_buffer.getvalue())

    # Check if the dataframe is empty or only has column names
    if files_status_df.empty or (len(files_status_df) == 0 and len(files_status_df.columns) > 0):
        print("DataFrame is empty or only has column names. Skipping save.")
        return

    # Retrieve the directory path from the connection
    dataPath = BaseHook.get_connection("fs_IpToolkitData").description
    file_path = os.path.join(dataPath, 'results/' + file_dir.split('/')[-1])
    if not os.path.exists(file_path):
        os.makedirs(file_path)

    csv_buffer = io.StringIO()
    files_status_df.to_csv(csv_buffer, index=False, sep=';', doublequote=True)
    
    csv_file_path = os.path.join(file_path, file_name)
    write_initial_file_status(csv_buffer, csv_file_path)
    
    print(f"File results written to {csv_file_path}")

def get_files(file_dir, remote_files):
    dataPath = BaseHook.get_connection("fs_IpToolkitData").description
    file_path = os.path.join(dataPath, file_dir, remote_files)
    
    try:
        with open(file_path, 'rb') as f:
            file_content = f.read()
        return file_content
        
    except Exception as e:
        print(f"Failed to process file {file_path}: {e}")

def save_files(file_dir, files_status_df, file_name):
    def write_initial_file_status(csv_buffer, csv_file_path):
        csv_buffer.seek(0)
        with open(csv_file_path, 'w') as local_file:
            local_file.write(csv_buffer.getvalue())

    # Retrieve the directory path from the connection
    dataPath = BaseHook.get_connection("fs_IpToolkitData").description
    file_path = os.path.join(dataPath, file_dir, file_name + '.csv')

    if not os.path.exists(dataPath):
        os.makedirs(dataPath)

    csv_buffer = io.StringIO()
    files_status_df.to_csv(csv_buffer, index=False)

    write_initial_file_status(csv_buffer, file_path)
    
    print(f"File status written to {file_path}")
    

def process_and_store(**kwargs):

    ti = kwargs['ti']
    result = ti.xcom_pull(key='initial_process_result', task_ids='task_dispatcher')

    if len(result[0])>0:
        log_id = result[0][0]
        file_dir = 'raw_data/' + result[0][1].split('||')[0]
        file_target = result[0][1].split('||')[1]

        print(log_id, file_dir, file_target)

    else:
        return

    def get_file_extrac(file_dir,file_name):
        try:
            keywords = [
            ['by ZTE Corporation', 'ZTE_Task'],
            ['Cisco IOS XR Software', 'Cisco_Task'],
            ['Huawei Versatile', 'Huawei_Task'],
            ['TiMOS', 'Nokia_Task'],
            ['HUAWEI ATN', 'Huawei_Task'],
            ]
            
            file_content = get_files(file_dir, file_name)

            listFile = []
            print('listing_file.....')
            with py7zr.SevenZipFile(io.BytesIO(file_content), mode="r") as archive:
                for info in archive.files:
                    if info.filename.startswith("inspect") and info.filename.endswith('.txt'):
                        if not(info.filename.split('/')[-1] == 'CollectResult.txt' or info.filename.split('/')[-1] == 'CollectSendDetail.txt'):
                        # if info.filename.split('/')[-1] == 'CommonCollectResult.txt':
                            listFile.append(info.filename)
                            
            print('done_listing_file.....')

            print('total_file_inside : ', len(listFile))

            print('mapping_file.....')
            list_map_vendor = []
            with py7zr.SevenZipFile(io.BytesIO(file_content), mode="r") as archive:
                for f, d in archive.read(listFile).items():
                    bytesData = d.read()
                    string_data = bytesData.decode('utf-8')
                    text_io = io.StringIO(string_data, newline=None)
                    try:
                        foundest = False
                        for i, line in enumerate(text_io):
                            if i >= 200:
                                break
                            for keyword in keywords:
                                if keyword[0] in line:
                                    list_map_vendor.append([f,keyword[1]])
                                    foundest = True
                                    break
                        if foundest == False:
                            list_map_vendor.append([f,np.nan])
                    except Exception as e:
                        list_map_vendor.append([f,np.nan])
                        
            print('done_mapping_file.....')

            df = pd.DataFrame(list_map_vendor, columns=['file_name', 'vendor'])     
            df['file_part'] = df['file_name'].apply(lambda x: x.split('/')[1])
            vendor_map = df.dropna(subset=['vendor']).set_index('file_part')['vendor'].to_dict()
            df['vendor'] = df.apply(
                lambda row: vendor_map[row['file_part']] if pd.isna(row['vendor']) and row['file_part'] in vendor_map else row['vendor'],
                axis=1
            )
            
            df = df.drop(columns=['file_part'])
            return df

        except Exception as e:
            print(e)
            raise AirflowException(f"Error occurred in MAPPER TASK: {str(e)}")
    
    file_names = f'{file_target}.nic'

    list_vendor = get_file_extrac(file_dir ,file_names)

    print('mapping_record_saved')

    save_files(file_dir, list_vendor, file_names.split('.')[0])

    list_vendor_no_na = list_vendor[['vendor']].dropna(ignore_index=True)

    unique_vendors = list_vendor_no_na['vendor'].unique()
    unique_vendors_list = list(unique_vendors)
    print(unique_vendors_list)

    processed_data = {
    'brach_init': unique_vendors_list,
    'file_name': file_names,
    'file_dir' : file_dir,
    'log_id' : log_id,
    }

    kwargs['ti'].xcom_push(key='processed_data', value=processed_data)

def update_database(**kwargs):
    processed_data = kwargs['ti'].xcom_pull(task_ids='File_Mapper', key='processed_data')
    data = processed_data['log_id']

    mysql_hook = MySqlHook(mysql_conn_id="main_db")
    try:
        sql = f"UPDATE data_log.iptoolkit_parser_log SET process_status = 'processed' WHERE id = {data}"
        mysql_hook.run(sql)
        print("Database updated successfully.")
    except Exception as e:
        print(f"Failed to update database: {e}")

def check_data_log(**kwargs):
    def checkDataLog():
        mainsqlserver = MySqlHook("main_db")
        sql = '''SELECT id, filename FROM data_log.iptoolkit_parser_log where process_status = 'not_processed' LIMIT 1;'''
        df = mainsqlserver.get_pandas_df(sql=sql)
        df.columns = df.columns.str.lower()
        r = df.values.tolist()  # Convert DataFrame to 2D array (list of lists)
        return r

    result = checkDataLog()
    if not result:
        print("No unprocessed data found. Marking DAG as done.")
        return 'clear'
    else:
        print("Unprocessed data found. Triggering DAG again.")
        return 'trigger_dag_again'


def create_dag(dag_id,args,dag_schedule):
    dag = DAG(dag_id, default_args=args, schedule_interval=dag_schedule, catchup=False, dagrun_timeout=pendulum.duration(hours=1),tags=["data","parser"])

    with dag:
        init = DummyOperator(
            task_id='Init',
            dag=dag
        )

        clear = DummyOperator(
            task_id='clear',
            dag=dag
        )

        dispacher = PythonOperator(
            task_id='task_dispatcher',
            python_callable=initial_process,
            retries= 0,
            retry_delay= pendulum.duration(minutes=2),                
            dag=dag
        )

        proc = PythonOperator(
            task_id='File_Mapper',
            python_callable=process_and_store,
            retries= 0,
            retry_delay= pendulum.duration(minutes=2),                
            dag=dag
        )    

        def branch_tasks(**kwargs):
            processed_data = kwargs['ti'].xcom_pull(task_ids='File_Mapper', key='processed_data')
            # branches = [task_label for task_label in processed_data['brach_init'] if dag.has_task(task_label)]

            branches = processed_data['brach_init']
            print(branches)
            return branches

        branch_task = BranchPythonOperator(
            task_id='branch_task',
            python_callable=branch_tasks,
            provide_context=True,
            dag=dag,
        )

        task2 = PythonOperator(
            task_id='ZTE_Task',
            python_callable=zte_process,
            op_kwargs={'task_label': 'ZTE_Task'},
            provide_context=True,
            dag=dag,
        )

        task3 = PythonOperator(
            task_id='Cisco_Task',
            python_callable=cisco_process,
            op_kwargs={'task_label': 'Cisco_Task'},
            provide_context=True,
            dag=dag,
        )

        task4 = PythonOperator(
            task_id='Huawei_Task',
            python_callable=huawei_process,
            op_kwargs={'task_label': 'Huawei_Task'},
            provide_context=True,
            dag=dag,
        )

        task5 = PythonOperator(
            task_id='Nokia_Task',
            python_callable=nokia_process,
            op_kwargs={'task_label': 'Nokia_Task'},
            provide_context=True,
            dag=dag,
        )

        join_task = DummyOperator(
            task_id='join_task',
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            dag=dag,
        )

        update_db_task = PythonOperator(
            task_id='update_status_task',
            python_callable=update_database,
            provide_context=True,
            dag=dag,
        )

        check_data_log_task = BranchPythonOperator(
            task_id='check_data_log_task',
            python_callable=check_data_log,
            provide_context=True,
            dag=dag,
        )

        trigger_dag_again_task = TriggerDagRunOperator(
            task_id='trigger_dag_again',
            trigger_dag_id='iptoolkit_parser_dag',  # ID of the DAG to trigger
            reset_dag_run=True,
            wait_for_completion=False,
            dag=dag,
        )

        init >> dispacher >> proc >> branch_task 
        branch_task >> [task2, task3, task4, task5] >> join_task >> update_db_task >> check_data_log_task
        check_data_log_task >> [clear, trigger_dag_again_task]

        return dag

# dag_schedule = "0 4-23 * * *"
# dag_schedule = "0 4-23 * * *"
dag_schedule = "@once"

args = {
        'concurrency': 1,
        'start_date': pendulum.datetime(2023, 11, 7, tz="Asia/Jakarta"),
        'depends_on_past' : False,
    }

create_dag("iptoolkit_parser_dag", args, dag_schedule)