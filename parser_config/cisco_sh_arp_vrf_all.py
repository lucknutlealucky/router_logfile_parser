import re
import pandas as pd
import numpy as np

def csc_sh_arp_vrf_all(papayukero,ff):
    try:
        parsing = False
        parsed_paragraph = []
        parsed_lines = []
        pewpewpew = []
    
        for i in papayukero:
            pewpewpew.append(i)
        
        for line in pewpewpew:
            if 'show arp vrf all' in line:
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
            ne_mac = []
            for mm in parsed_paragraph:
                parsing = False
                header_skip = False
                macs = []
                router = ''
                para_counter = 0
                for qq in mm:
                    if '---' in qq and parsing == False and header_skip == False:
                        header_skip = True
                        continue
                    elif re.search(r'[A-Za-z0-9-]*#', qq):
                        match = re.search(r'[A-Za-z0-9-]*#', qq)
                        if match:
                            router = match.group().replace(':','').replace('#','')
                            parsing = False
        
                    elif '---' in qq and parsing == False and header_skip == True:
                        header_skip = False
                        parsing = True
                        continue
                        
                    elif parsing and (header_skip == False):
                        used = qq.replace('\n','')
                        stopper = len(used) 
                        if(used.split() == 6):
                            ii = used.split()
                            macs.append([ii[0],ii[1],ii[2],ii[3],ii[4],ii[5],ii[6]])
                            para_counter = para_counter + 1
                        else:
                            macs.append([''.join(used[0:16]).replace(' ',''),
                                          ''.join(used[16:27]).replace(' ',''),
                                          ''.join(used[27:43]).replace(' ',''),
                                          ''.join(used[43:54]).replace(' ',''),
                                          ''.join(used[54:59]).replace(' ',''),
                                          ''.join(used[59:stopper]).replace(' ',''),
                                          ''.join(used[80:stopper]).replace(' ','')])
            
                for uu in macs:
                    ne_mac.append([router,uu[0],uu[1],uu[2],uu[3],uu[4],uu[5]])
        
            data = ne_mac[1:]
            df2 = pd.DataFrame(data, columns=['HOSTNAME','Address','Age','Hardware_Addr','State','Type','Interface'])
            df2 = df2[~(
                            ((df2['Address'] == '') & (df2['Age'] == '') & 
                            (df2['Hardware_Addr'] == '') & (df2['State'] == '')) | 
                            (pd.isna(df2['Address']) & pd.isna(df2['Age']) & 
                            pd.isna(df2['Hardware_Addr']) & pd.isna(df2['State'])))]

            df2 = df2[~(df2['Address'].str.contains('---'))]
            df2 = df2[~(df2['Address'].str.contains('CPU'))]
            df2 = df2[~(df2['Address'].str.contains('Address'))]
            
            return df2
        else:
            columns = ['HOSTNAME','Address','Age','Hardware_Addr','State','Type','Interface']
            df2 = pd.DataFrame(columns=columns)
            return df2
                        
    except Exception as e:
        print(ff)
        # print(parsed_paragraph)
        # print(ii)
        # print(mm)
        # print(i)
        # print(e)
        raise
            