import re
import pandas as pd
import numpy as np

def l2vpn_fwd_brdg_mac_loc(papayukero,ff):
    try:
        parsing = False
        parsed_paragraph = []
        parsed_lines = []
        pewpewpew = []
    
        for i in papayukero:
            pewpewpew.append(i)
        
        for line in pewpewpew:
            if 'show l2vpn forwarding bridge-domain mac-address location 0/0/CPU0' in line or 'show l2vpn forwarding bridge-domain mac-address location 0/RP0/CPU0' in line:
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
            ne_interface = []
            for mm in parsed_paragraph:
                parsing = False
                header_skip = False
                router = ''
                interface = []
                for qq in mm:
                    if '---' in qq and parsing == False:
                        parsing = True
                        continue
                        
                    elif re.search(r'[A-Za-z0-9-]*#', qq):
                        match = re.search(r'[A-Za-z0-9-]*#', qq)
                        if match:
                            router = match.group().replace(':','').replace('#','')
                            parsing = False
                            
                    elif parsing and (header_skip == False):
                        stopper = len(qq)
                        m = qq.replace('\n','')
                        interface.append([''.join(m[0:15]).replace(' ',''),
                                          ''.join(m[15:23]).replace(' ',''),
                                          ''.join(m[23:50]).replace(' ',''),
                                          ''.join(m[50:62]).replace(' ',''),
                                          ''.join(m[62:85]).replace(' ',''),
                                          ''.join(m[85:stopper]).replace(' ','')])
                for ll in interface:
                    ne_interface.append([router,ll[0],ll[1],ll[2],ll[3],ll[4],ll[5]])

                # print(ne_interface)
            
                df1 = pd.DataFrame(ne_interface, columns=['HOSTNAME','Mac Address', 'Type', 'Learned from/Filtered on', 'LC learned', 'Resync Age/Last Change', 'Mapped to'])
                # df1 = df[(df['Type'] == 'dynamic') & ~(df['Learned from/Filtered on'].str.match(r'\(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3},\d+\)'))].reset_index(drop=True)
                return df1
        else:
            df1 = pd.DataFrame(columns=['HOSTNAME','Mac Address', 'Type', 'Learned from/Filtered on', 'LC learned', 'Resync Age/Last Change', 'Mapped to'])
            return df1
                                    
    except Exception as e:
        print(ff)
        # print(parsed_paragraph)
        # print(ii)
        # print(mm)
        # print(i)
        # print(e)
        raise
            