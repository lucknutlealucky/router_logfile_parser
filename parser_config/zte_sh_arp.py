import re
import pandas as pd
import numpy as np

def zte_sh_arp(papayukero,ff):
    try:
        parsing = False
        parsed_paragraph = []
        parsed_lines = []
        pewpewpew = []
    
        for i in papayukero:
            pewpewpew.append(i)
        
        for line in pewpewpew:
            if 'show arp' in line:
                parsing = True
                parsed_lines.append(line)
            elif (parsing and re.search(r'[A-Za-z0-9-]*#', line)): 
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
                
                pattern = r'[A-Za-z0-9-]*#'
                router = ''
                
                for line in papa:
                    if first and '---' in line:
                        header_found = True
                        first = False
                        continue
                    elif header_found and (re.search(r'[A-Za-z0-9-]*#', line)): 
                        match = re.search(pattern, line)
                        if match:
                            router = match.group().replace('#','')
                            header_found = False
                        continue
                    elif header_found:
                        filtered_data.append(line)
                        
                touse = []
                touse.append(["ROUTER", "IP_ADDRESS", "AGE", "PHY_ADDRESS", "INTERFACE", "EXTTERNAL_VLANID", "INTERNAL_VLANID", "SUB_INTERFACE"])
                para_counter = 1
                for tt in filtered_data:
                    if len(tt.split()) == 7:
                        qqq = tt.replace('\n','')
                        lll = qqq.split()
                        touse.append([router,lll[0],lll[1],lll[2],lll[3],lll[4],lll[5],lll[6]])
                        para_counter = para_counter + 1

                    elif len(tt.split()) < 3:
                        qqq = tt.replace('\n','')
                        ii = qqq.split()
                        for kk in ii:
                            if kk.startswith('.') or kk.startswith('/'):
                                touse[para_counter - 1][4] = touse[para_counter - 1][4] + kk
                            elif touse[para_counter - 1][7][-1] == '.' or touse[para_counter - 1][7][-1] == '/':
                                touse[para_counter - 1][7] = touse[para_counter - 1][7] + kk
                    else:
                        stopper = len(tt)
                        m = tt.replace('\n','')
                        touse.append([router,
                            ''.join(m[0:16]).replace(' ',''),
                            ''.join(m[16:25]).replace(' ',''),
                            ''.join(m[25:40]).replace(' ',''),
                            ''.join(m[40:53]).replace(' ',''),
                            ''.join(m[53:60]).replace(' ',''),
                            ''.join(m[60:67]).replace(' ',''),
                            ''.join(m[67:stopper]).replace(' ','')])
                        para_counter = para_counter + 1
                        
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
                                    
    except Exception as e:
        print(ff)
        # print(parsed_paragraph)
        # print(ii)
        # print(mm)
        # print(i)
        # print(e)
        raise
            