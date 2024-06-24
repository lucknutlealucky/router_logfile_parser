import re
import pandas as pd
import os
import fnmatch
import numpy as np

def get_hw_ospf(polimas):
    parsing = False
    parsed_paragraph = []
    parsed_lines = []
    
    for line in polimas:
        if 'display ospf peer brief' in line:
            parsing = True
            parsed_lines.append(line)
        elif (parsing and re.search(r'<(.*?)>', line)): 
            parsing = False
            parsed_lines.append(line)
            parsed_paragraph.append(parsed_lines)
            parsed_lines = []
        elif parsing:
            parsed_lines.append(line)

    
    if len(parsed_paragraph) > 0 and len(parsed_paragraph[0]) > 3: 
        header_found = False
        parse_state = False
        filtered_data = []
        first = True
        
        pattern = r"<([^>]+)>"
        router = ''
        
        for parsed_paragraphz in parsed_paragraph:
            data_temp = []
            for line in parsed_paragraphz:
                if first and 'OSPF Process' in line:
                    header_found = True
                    first = False
                    parse_state = True
                    data_temp.append(line)
                elif (header_found == True) and r'---' in line: 
                    header_found = False
                    data_temp.append(line)
                elif (header_found == False) and r'---' in line: 
                    parse_state = False
                    first = True
                    data_temp.append(line)
                    filtered_data.append(data_temp)
                    data_temp = []
                elif parse_state:
                    data_temp.append(line)
                    
                match = re.search(pattern, line)
                if match:
                    router = match.group(1)
        df_bucket = []
        for mike in filtered_data:
            
            text = ''.join(mike)
            ospf_match = re.search(r"OSPF Process (\d+) with Router ID ([\d\.]+)", text)
            ospf_process = ospf_match.group(1)
            router_id = ospf_match.group(2)

            table_data = []
            lines = text.splitlines()
            data_start = False

            pattern = re.compile(r"(\S+)\s+(\S+)\s+([\d\.]+)\s+(\S+)")

            for line in lines:
                if 'Area Id' in line:
                    data_start = True
                    continue
                
                if data_start:
                    if re.match(r"\s*-+\s*", line): 
                        data_start = False
                        continue
                    
                    match = pattern.search(line)
                    if match:
                        area_id, interface, neighbor_id, state = match.groups()
                        table_data.append([area_id, interface, neighbor_id, state])

            df = pd.DataFrame(table_data, columns=['Area_Id', 'Interface', 'Neighbor_id', 'State'])

            df['OSPF_Process'] = ospf_process
            df['Router_ID'] = router_id
            df['Host_Name'] = router
            df = df[['Host_Name', 'OSPF_Process', 'Router_ID', 'Area_Id', 'Interface', 'Neighbor_id', 'State']]
            
            df_bucket.append(df)
        return pd.concat(df_bucket)


def get_hw_bgp(polimas):
    parsing = False
    parsed_paragraph = []
    parsed_lines = []

    pattern = r"<([^>]+)>"
    router = ''

    for line in polimas:
        if 'display bgp vpnv4 all peer' in line:
            parsing = True
            # parsed_lines.append(line)
        elif (parsing and re.search(r'<(.*?)>', line)): 
            parsing = False
            parsed_lines.append(line)
            parsed_paragraph.append(parsed_lines)
            parsed_lines = []
            
            match = re.search(pattern, line)
            if match:
                router = match.group(1)
                
        elif parsing:
            parsed_lines.append(line)
    
    if len(parsed_paragraph) > 0 and len(parsed_paragraph[0]) > 3: 

        buket_kembang= []
        for epew in parsed_paragraph:
            lines = ''.join(epew).strip().split("\n")
            router_id = None
            local_as = None
            vpn_instance = None
            vpn_router_id = None
            peer_data = []
            table_pattern = re.compile(r"\s+")

            for line in lines:
                line = line.strip()
                if "BGP local router ID" in line:
                    router_id = re.search(r"(\d+\.\d+\.\d+\.\d+)", line).group(1)
                elif "Local AS number" in line:
                    local_as = re.search(r"(\d+)", line).group(1)
                elif "VPN-Instance" in line:
                    vpn_instance = re.search(r"VPN-Instance (\S+)", line).group(1)
                    vpn_router_id = re.search(r"Router ID (\d+\.\d+\.\d+\.\d+)", line).group(1)
                elif line.startswith("Peer") or "Peer of IPv4-family" in line or not line:
                    continue
                else:
                    columns = re.split(table_pattern, line)
                    if len(columns) >= 9 and not (columns[0] == 'Total'):
                        columns.extend([""] * (9 - len(columns)))
                        peer_info = {
                            "Host_Name" : router,
                            "BGP_local_router_ID": router_id,
                            "Local_AS_number": local_as,
                            "Peer": columns[0],
                            "v": columns[1],
                            "AS": columns[2],
                            "MsgRcvd": columns[3],
                            "MsgSent": columns[4],
                            "OutQ": columns[5],
                            "Up/Down": columns[6],
                            "State": columns[7],
                            "PrefRcv": columns[8],
                            "VPN-Instance": vpn_instance,
                            "VPN_Router_ID": vpn_router_id,
                        }
                        peer_data.append(peer_info)
            df = pd.DataFrame(peer_data)
            buket_kembang.append(df)
        return pd.concat(buket_kembang)