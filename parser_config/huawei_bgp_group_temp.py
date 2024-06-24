import re
import pandas as pd
import os
import sys


def parse_peer_info(papayukero):
    parsing = False
    parsed_paragraph = []
    parsed_lines = []
    
    print('parsing peer info', papayukero)

    with open(papayukero, 'r') as file:
        for line in file:
            if 'disp bgp vpnv4 all peer' in line:
                parsing = True
                parsed_lines.append(line)
            elif (parsing and 'disp current-configuration  configuration bgp | begin ipv4' in line): 
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
                
                for line in papa:
                    if first and ('Peer' in line and 'MsgRcvd' in line):
                        header_found = True
                        first = False
                        continue
                    elif re.search(r'<\w+-\w+-\w+-\w+-\w+>|<\w+-\w+-\w+-\w+>', line): 
                        match = re.search(r'<\w+-\w+-\w+-\w+-\w+>|<\w+-\w+-\w+-\w+>', line)
                        if match:
                            router = match.group().replace('<','').replace('>','')
                            header_found = False
                        continue
                    elif header_found:
                        filtered_data.append(line)
                touse = []
                touse.append(["HOST_NE","Peer", "V", "AS", "MsgRcvd", "MsgSent",'OutQ','Up/Down','State','PrefRcv'])
                for m in filtered_data:
                    stopper = len(m)
                    m = m.replace('\n','')
                    parsed_data = m.split()
                    try:
                        touse.append([router, parsed_data[0], parsed_data[1], parsed_data[2],
                                      parsed_data[3], parsed_data[4], parsed_data[5],
                                      parsed_data[6], parsed_data[7], parsed_data[8],])
                    except:
                        pass
                header = touse[0]
                data = touse[1:]
                df = pd.DataFrame(data, columns=header)
                dferr.append(df)
            df = pd.concat(dferr, ignore_index=True)
            return df
    else:
        columns = ["HOST_NE","Peer", "V", "AS", "MsgRcvd", "MsgSent",'OutQ','Up/Down','State','PrefRcv']
        df = pd.DataFrame(columns=columns)
        return df

def parse_group_info(papayukeri):
    parsing = False
    parsed_paragraph = []
    parsed_lines = []
    
    print('parsing group info', papayukeri)
    
    pattern = re.compile(r'\b(?:TEMP(?:_PRIO\w*)?|TEMP_PRIO)\b')
    with open(papayukeri, 'r') as file:
        for line in file:
            if 'disp current-configuration  configuration bgp | begin ipv4' in line:
                parsing = True
                # parsed_lines.append(line)
            elif (parsing and re.search(r'<\w+-\w+-\w+-\w+-\w+>|<\w+-\w+-\w+-\w+>', line)): 
                parsing = False
                if len(parsed_lines)>3 :
                    match = re.search(r'<\w+-\w+-\w+-\w+-\w+>|<\w+-\w+-\w+-\w+>', line)
                    if match:
                        router = match.group().replace('<','').replace('>','')
                        parsed_lines.append(router)
                    parsed_paragraph.append(parsed_lines)
                parsed_lines = []
            elif parsing and (pattern.search(line) and 'group' in line):
                if len(line.split())>3:
                    parsed_lines.append(line)
    
    if len(parsed_paragraph) > 0 and len(parsed_paragraph[0]) > 0: 
            dferr = []
            for papa in parsed_paragraph:
                touse = []
                touse.append(["HOST_NE","PEER","GROUP"])
                router = papa[-1]
                for m in papa[:-1]:
                    stopper = len(m)
                    m = m.replace('\n','')
                    parsed_data = m.split()
                    try:
                        touse.append([router, parsed_data[1], parsed_data[2]+ " " + parsed_data[3]])
                    except:
                        pass
                header = touse[0]
                data = touse[1:]
                df = pd.DataFrame(data, columns=header)
                dferr.append(df)

            if dferr and any(df.shape[0] > 0 for df in dferr if isinstance(df, pd.DataFrame)):
                df = pd.concat(dferr, ignore_index=True)
            else:
                columns = ["HOST_NE","IP","GROUP"]
                df = pd.DataFrame(columns=columns)
            return df
    else:
        columns = ["HOST_NE","IP","GROUP"]
        df = pd.DataFrame(columns=columns)
        return df