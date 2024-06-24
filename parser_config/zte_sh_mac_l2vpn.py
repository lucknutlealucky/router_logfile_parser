import re
import pandas as pd
import numpy as np

def zte_sh_mac_l2vpn(papayukero,ff):
    try:
        parsing = False
        parsed_paragraph = []
        parsed_lines = []
        pewpewpew = []
        hostname = ''
        pattern = r"\b\d{2}:\d{2}:\d{2}\s+\w*\s+\w{3}\s+\w{3}\s+\d{2}\s+\d{4}\b"
    
        for i in papayukero:
            pewpewpew.append(i)
        
        for line in pewpewpew:
            if re.match(pattern, line):
                continue
                
            if 'show mac l2vpn' in line:
                parsing = True
                # parsed_lines.append(line)
            elif (parsing and re.search(r'^[A-Za-z0-9-]*#', line)): 
                parsing = False
                parsed_lines.append(line)
                parsed_paragraph.append(parsed_lines)
                parsed_lines = []
                hostname = line.replace('#', '').replace('\n','')
            elif parsing:
                parsed_lines.append(line)

        
        if len(parsed_paragraph) > 0 and len(parsed_paragraph[0]) > 0: 
            dferr = []
            for papa in parsed_paragraph:
            
                header_found = False
                filtered_data = []
                first = True
                
                pattern = r'^[A-Za-z0-9-]*#'
                router = ''
                
                for line in papa:
                    if first and '---' in line:
                        header_found = True
                        first = False
                        continue
                    elif header_found and (re.search(r'^[A-Za-z0-9-]*#', line)): 
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
                                    
    except Exception as e:
        print(ff)
        print(parsed_paragraph)
        # print(ii)
        # print(mm)
        # print(i)
        # print(e)
        raise
            