import re
import pandas as pd

def nk_sh_router_arp(papayukero,ff):
    try:
        parsing = False
        parsed_paragraph = []
        parsed_lines = []
        pewpewpew = []

        for i in papayukero:
            pewpewpew.append(i)

        for line in pewpewpew:
            if 'ARP Table (Router:' in line:
                parsing = True
                parsed_lines.append(line)
            elif (parsing and (re.search(r'\w+:[A-Za-z0-9-]*#', line))): 
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
                invalid = False
                
                pattern = r'\w+:[A-Za-z0-9-]*#'
                router = ''
                vrf = ''
                
                for line in papa:
                    if first and '---' in line:
                        header_found = True
                        first = False
                        continue
                    elif '---' in line and first == False:
                        header_found = False
                    elif 'No Matching Entries Found' in line:
                        invalid = True
                    elif (re.search(r'\w+:[A-Za-z0-9-]*#', line)): 
                        try:
                            # router = match.group().replace('#','').split(':')[1]
                            router = line.replace('#','').split(':')[1].replace('\n','').replace(' ','')
                        except:
                            # router = match.group().replace('#','')
                            router = line.replace('#','').replace('\n','').replace(' ','')
                        header_found = False
                        continue
                    elif header_found == False and 'ARP Table (Router:' in line: 
                        vrf = line.split(':')[1].replace(' ','').replace(')\n','')
                        continue
                    elif header_found:
                        filtered_data.append(line)
                        
                if invalid:
                    continue
            
                touse = []
                touse.append(["HOST_NAME","Router", "IP_ADDRESS", "MAC_ADRESS", "EXPIRY", "TYPE", "INTERFACE"])
                for m in filtered_data:
                    stopper = len(m)
                    m = m.replace('\n','')
                    touse.append([router,vrf,
                          ''.join(m[0:16]).replace(' ',''),
                          ''.join(m[16:34]).replace(' ',''),
                          ''.join(m[34:44]).replace(' ',''),
                          ''.join(m[44:51]).replace(' ',''),
                          ''.join(m[51:stopper]).replace(' ','')])
                
                header = touse[0]
                data = touse[1:]
                df = pd.DataFrame(data, columns=header)
                dferr.append(df)
            if dferr and any(df.shape[0] > 0 for df in dferr if isinstance(df, pd.DataFrame)):
                df = pd.concat(dferr, ignore_index=True)
            else:
                columns = ["HOST_NAME","Router", "IP_ADDRESS", "MAC_ADRESS", "EXPIRY", "TYPE", "INTERFACE"]
                df = pd.DataFrame(columns=columns)
    
            return df
        else:
            columns = ["HOST_NAME","Router", "IP_ADDRESS", "MAC_ADRESS", "EXPIRY", "TYPE", "INTERFACE"]
            df = pd.DataFrame(columns=columns)
            return df
        
                        
    except Exception as e:
        print(ff)
        # print(parsed_paragraph)
        # print(ii)
        # print(mm)
        # print(i)
        # print(e)
        raise
            