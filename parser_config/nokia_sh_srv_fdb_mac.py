import re
import pandas as pd
import numpy as np

def nk_sh_srv_fdb_mac(papayukero,ff):
    def is_integer(s):
        try:
            int(s)
            return True
        except ValueError:
            return False
    def is_mac_like_address(s):
        mac_pattern = re.compile(r'^([0-9a-fA-F]{2}:){9}[0-9a-fA-F]{2}$')
        return bool(mac_pattern.match(s))

    def is_ip_with_port(s):
        ip_port_pattern = re.compile(r'^(\d{1,3}\.){3}\d{1,3}:\d{1,6}$')
        return bool(ip_port_pattern.match(s))
        
    try:
        parsing = False
        parsed_paragraph = []
        parsed_lines = []
        pewpewpew = []

        for i in papayukero:
            pewpewpew.append(i)
        
        for line in pewpewpew:
            if 'show service fdb-mac' in line:
                parsing = True
                parsed_lines.append(line)
            elif (parsing and (re.search(r'^\**[A-Z]*:[A-Za-z0-9-]*\s*[A-Za-z0-9>]*\s*#\s*', line))):  
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
                
                router = ''
                
                for line in papa:
                    if first and '---' in line:
                        header_found = True
                        first = False
                        continue
                    elif '---' in line and first == False:
                        header_found = False
                    elif re.search(r'^\**[A-Z]*:[A-Za-z0-9-]*\s*[A-Za-z0-9>]*\s*#\s*', line): 
                        try:
                            # router = match.group().replace('#','').split(':')[1]
                            router = line.replace('#','').split(':')[1].replace('\n','').replace(' ','')
                        except:
                            # router = match.group().replace('#','')
                            router = line.replace('#','').replace('\n','').replace(' ','')
                        header_found = False
                        continue
                    elif header_found:
                        filtered_data.append(line)
                        
                    
                
                touse = []
                touse.append(["HOST_NAME","SERVICE_ID", "MAC", "SOURCE_IDENTIFIER", "TYPE_AGE", "LAST_CHANGE"])

                indexer = 1
                for m in filtered_data:
                    stopper = len(m)
                    if len(m.rstrip().split()) == 5:
                        popod = m.rstrip().split()
                        touse.append([router,popod[0],popod[1],popod[2],popod[3],popod[4]])
                        indexer = indexer + 1
                        
                    elif len(m.rstrip().split()) == 1:
                        popod = m.rstrip().split()[0]
                        if is_integer(popod):
                            touse.append([router,popod[0],'','','',''])
                            indexer = indexer + 1
                            
                        elif popod.upper() == 'P':
                            touse[indexer - 1][4] = touse[indexer - 1][4] + ' P'
                            
                        elif is_mac_like_address(popod) or is_ip_with_port(popod):
                            touse[indexer - 1][3] = touse[indexer - 1][3] + popod
                            
                    else:
                        m = m.replace('\n','')
                        mouse = [router,
                              ''.join(m[0:10]).replace(' ',''),
                              ''.join(m[10:28]).replace(' ',''),
                              ''.join(m[28:53]).replace(' ',''),
                              ''.join(m[53:62]).replace(' ',''),
                              ''.join(m[62:stopper]).replace(' ','')]

                        touse.append(mouse)
                        indexer = indexer + 1
                
                header = touse[0]
                data = touse[1:]
                df = pd.DataFrame(data, columns=header)
                dferr.append(df)
                
            if dferr and any(df.shape[0] > 0 for df in dferr if isinstance(df, pd.DataFrame)):
                df = pd.concat(dferr, ignore_index=True)
            else:
                columns = ["HOST_NAME","SERVICE_ID", "MAC", "SOURCE_IDENTIFIER", "TYPE_AGE", "LAST_CHANGE"]
                df = pd.DataFrame(columns=columns)
            return df
        else:
            columns = ["HOST_NAME","SERVICE_ID", "MAC", "SOURCE_IDENTIFIER", "TYPE_AGE", "LAST_CHANGE"]
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
            