import re
import pandas as pd
import os
import fnmatch
import numpy as np

def get_dataframe_huawei_arp(polimas):
    parsing = False
    parsed_paragraph = []
    parsed_lines = []

    for line in polimas:
        if 'display arp all' in line:
            parsing = True
            parsed_lines.append(line)
        elif (parsing and re.search(r'<(.*?)>', line)): 
            parsing = False
            parsed_lines.append(line)
            parsed_paragraph.append(parsed_lines)
            parsed_lines = []
        elif parsing:
            parsed_lines.append(line)

    if len(parsed_paragraph) > 0 and len(parsed_paragraph[0]) > 0: 
        
        header_found = False
        filtered_data = []
        first = True
        
        pattern = r"<([^>]+)>"
        router = ''
        
        for parsed_paragraphz in parsed_paragraph:
            for line in parsed_paragraphz:
                if first and '---' in line:
                    header_found = True
                    first = False
                    continue
                elif header_found and r'---' in line: 
                    header_found = False
                    continue
                elif header_found:
                    filtered_data.append(line)
                    
                match = re.search(pattern, line)
                if match:
                    router = match.group(1)
        
        touse = []
        touse.append(["ROUTER", "IP_ADDRESS", "MAC_ADDRESS", "EXPIRE(M)", "TYPE_VLAN/CEVLAN", "INTERFACE_PVC", "VPN-INSTANCE"])

        # if router == 'JKT-TCP-CN1-H8X08':
            # print(polimas)
        for m in filtered_data:
            if len(m.split()) > 4:
                mama = m.split()
                try:
                    if mama[2] == 'I':
                        touse.append([router, mama[0], mama[1], np.nan, mama[2] + mama[3], mama[4], mama[5]])
                    elif 'S' in mama[2]:
                        touse.append([router, mama[0], mama[1], np.nan, mama[2], mama[3], mama[4]])
                    else:
                        touse.append([router, mama[0], mama[1], mama[2], mama[3], mama[4], mama[5]])
                    
                except:
                    if mama[2] == 'I':
                        touse.append([router, mama[0], mama[1], np.nan, mama[2] + mama[3], mama[4], np.nan])
                    elif 'S' in mama[2]:
                        touse.append([router, mama[0], mama[1], np.nan, mama[2], np.nan, mama[3]])
                    else:
                        touse.append([router, mama[0], mama[1], mama[2], mama[3], mama[4], np.nan])

            else:
                stopper = len(m)
                m = m.replace('\n','')
                touse.append([router, ''.join(m[0:16]).replace(' ',''), ''.join(m[16:32]).replace(' ',''), ''.join(m[32:41]).replace(' ',''),
                      ''.join(m[41:54]).replace(' ',''), ''.join(m[54:71]).replace(' ',''), ''.join(m[71:stopper]).replace(' ','')])
        
        header = touse[0]
        data = touse[1:]
        df = pd.DataFrame(data, columns=header)

        for index in range(len(df) - 1, 0, -1):
                if pd.isna(df.at[index, 'MAC_ADDRESS']) or df.at[index, 'MAC_ADDRESS'] == '':
                    if index > 0:
                        df.at[index - 1, 'TYPE_VLAN/CEVLAN'] += ( ' '  + df.at[index, 'TYPE_VLAN/CEVLAN'])

        df = df.replace("",np.nan)
        df = df.dropna(subset=['IP_ADDRESS','MAC_ADDRESS','INTERFACE_PVC','VPN-INSTANCE'],how='all').reset_index(drop=True)

        return df

    else:
        columns = ["ROUTER", "IP_ADDRESS", "MAC_ADDRESS", "EXPIRE(M)", "TYPE_VLAN/CEVLAN", "INTERFACE_PVC", "VPN-INSTANCE"]
        df = pd.DataFrame(columns=columns)
        return df
