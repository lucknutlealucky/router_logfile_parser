import re
import pandas as pd
import os
import fnmatch
import numpy as np

def zte_arp_parser(polimas):
    parsing = False
    parsed_paragraph = []
    parsed_lines = []
    print('parsing ARP',polimas)
    
    with open(polimas, 'r') as file:
        for line in file:
            if 'show arp' in line:
                parsing = True
                parsed_lines.append(line)
            elif (parsing and (re.search(r'\w+-\w+-\w+-\w+#', line) or re.search(r'\w+-\w+-\w+-\w+-\w+#', line))): 
                parsing = False
                parsed_lines.append(line)
                parsed_paragraph.append(parsed_lines)
                parsed_lines = []
            elif parsing:
                parsed_lines.append(line)
    
    if len(parsed_paragraph) > 0 and len(parsed_paragraph[0]) > 0: 

        dferr = []
        for papa in parsed_paragraph:
        
            header_found = False
            filtered_data = []
            first = True
            
            pattern = r'\w+-\w+-\w+-\w+#|\w+-\w+-\w+-\w+-\w+#'
            router = ''
            
            for line in papa:
                if first and '---' in line:
                    header_found = True
                    first = False
                    continue
                elif header_found and (re.search(r'\w+-\w+-\w+-\w+#', line) or re.search(r'\w+-\w+-\w+-\w+-\w+#', line)): 
                    match = re.search(pattern, line)
                    if match:
                        router = match.group().replace('#','')
                        header_found = False
                    continue
                elif header_found:
                    filtered_data.append(line)
                    
                
            
            touse = []
            touse.append(["ROUTER", "IP_ADDRESS", "AGE", "PHY_ADDRESS", "INTERFACE", "EXTTERNAL_VLANID", "INTERNAL_VLANID", "SUB_INTERFACE"])
            for m in filtered_data:
                stopper = len(m)
                m = m.replace('\n','')
                touse.append([router,
                    ''.join(m[0:16]).replace(' ',''),
                    ''.join(m[16:25]).replace(' ',''),
                    ''.join(m[25:40]).replace(' ',''),
                    ''.join(m[40:53]).replace(' ',''),
                    ''.join(m[53:60]).replace(' ',''),
                    ''.join(m[60:67]).replace(' ',''),
                    ''.join(m[67:stopper]).replace(' ','')])
            
            header = touse[0]
            data = touse[1:]
            df = pd.DataFrame(data, columns=header)
            dferr.append(df)
        if dferr and any(df.shape[0] > 0 for df in dferr if isinstance(df, pd.DataFrame)):
            df = pd.concat(dferr, ignore_index=True)
        else:
            columns = ["ROUTER", "IP_ADDRESS", "AGE", "PHY_ADDRESS", "INTERFACE", "EXTTERNAL_VLANID", "INTERNAL_VLANID", "SUB_INTERFACE"]
            df = pd.DataFrame(columns=columns)
        return df
    else:
        columns = ["ROUTER", "IP_ADDRESS", "AGE", "PHY_ADDRESS", "INTERFACE", "EXTTERNAL_VLANID", "INTERNAL_VLANID", "SUB_INTERFACE"]
        df = pd.DataFrame(columns=columns)
        return df

def zte_parse_l2vpn(polimas):
    parsing = False
    parsed_paragraph = []
    parsed_lines = []

    print('parsing Layer 2 VPN',polimas)
    
    with open(polimas, 'r') as file:
        for line in file:
            if 'show mac l2vpn' in line:
                parsing = True
                parsed_lines.append(line)
            elif (parsing and (re.search(r'\w+-\w+-\w+-\w+#', line) or re.search(r'\w+-\w+-\w+-\w+-\w+#', line))): 
                parsing = False
                parsed_lines.append(line)
                parsed_paragraph.append(parsed_lines)
                parsed_lines = []
            elif parsing:
                parsed_lines.append(line)
    
    if len(parsed_paragraph) > 0 and len(parsed_paragraph[0]) > 0: 


        dferr = []
        for papa in parsed_paragraph:
        
            header_found = False
            filtered_data = []
            first = True
            
            pattern = r'\w+-\w+-\w+-\w+#|\w+-\w+-\w+-\w+-\w+#'
            router = ''
            
            for line in papa:
                if first and '---' in line:
                    header_found = True
                    first = False
                    continue
                elif header_found and (re.search(r'\w+-\w+-\w+-\w+#', line) or re.search(r'\w+-\w+-\w+-\w+-\w+#', line)): 
                    match = re.search(pattern, line)
                    if match:
                        router = match.group().replace('#','')
                        header_found = False
                    continue
                elif header_found:
                    filtered_data.append(line)
                    
            touse = []
            touse.append(["ROUTER", "MAC_ADDRESS", "VPN", "VLAN", "Outgoing_Information", "Attribute"])
            for m in filtered_data:
                stopper = len(m)
                m = m.replace('\n','')
                touse.append([router,
                    ''.join(m[0:15]).replace(' ',''),
                    ''.join(m[15:30]).replace(' ',''),
                    ''.join(m[30:35]).replace(' ',''),
                    ''.join(m[35:68]).replace(' ',''),
                    ''.join(m[68:stopper]).replace(' ','')])
            
            header = touse[0]
            data = touse[1:]
            df = pd.DataFrame(data, columns=header)
            
            for index in range(len(df) - 1, 0, -1):
                if pd.isna(df.at[index, 'MAC_ADDRESS']) or df.at[index, 'MAC_ADDRESS'] == '':
                    if index > 0:
                        df.at[index - 1, 'VPN'] += df.at[index, 'VPN']
                    
            df = df.replace("",np.nan)
            df = df.dropna(subset=['MAC_ADDRESS','VLAN','Outgoing_Information','Attribute'],how='all').reset_index(drop=True)
            dferr.append(df)

        if dferr and any(df.shape[0] > 0 for df in dferr if isinstance(df, pd.DataFrame)):
            df = pd.concat(dferr, ignore_index=True)

        else:
            columns = ["ROUTER", "MAC_ADDRESS", "VPN", "VLAN", "Outgoing_Information", "Attribute"]
            df = pd.DataFrame(columns=columns)
        return df
    else:
        columns = ["ROUTER", "MAC_ADDRESS", "VPN", "VLAN", "Outgoing_Information", "Attribute"]
        df = pd.DataFrame(columns=columns)
        return df