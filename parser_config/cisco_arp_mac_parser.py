import re
import pandas as pd
import os
import sys
import fnmatch


def parse_peer_info_cisco(the_txt_file):
    parsing = False
    parsed_paragraph = []
    parsed_lines = []

    papayukero = the_txt_file
    
    print('parsing peer info', papayukero)

    with open(papayukero, 'r') as file:
        for line in file:
            if 'show l2vpn forwarding bridge-domain mac-address location 0/0/CPU0' in line or 'show l2vpn forwarding bridge-domain mac-address location 0/RP0/CPU0' in line:
                parsing = True
                parsed_lines.append(line)
            elif (parsing and re.search(r':\w+-\w+-\w+-\w+-\w+#|:\w+-\w+-\w+-\w+#', line)): 
                parsing = False
                parsed_lines.append(line)
                parsed_paragraph.append(parsed_lines)
                parsed_lines = []
            elif parsing:
                parsed_lines.append(line)

    # print(parsed_paragraph)
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
                
            elif re.search(r':\w+-\w+-\w+-\w+-\w+#|:\w+-\w+-\w+-\w+#', qq):
                match = re.search(r':\w+-\w+-\w+-\w+-\w+#|:\w+-\w+-\w+-\w+#', qq)
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

    df = pd.DataFrame(ne_interface, columns=['HOSTNAME','Mac Address', 'Type', 'Learned from/Filtered on', 'LC learned', 'Resync Age/Last Change', 'Mapped to'])

    df1 = df[(df['Type'] == 'dynamic') & ~(df['Learned from/Filtered on'].str.match(r'\(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3},\d+\)'))].reset_index(drop=True)

    if len(parsed_paragraph) > 0 and len(parsed_paragraph[0]) > 0:
        parsing = False
        parsed_paragraph = []
        parsed_lines = []
    
        with open(papayukero, 'r') as file:
            for line in file:
                if 'show arp vrf all' in line:
                    parsing = True
                    parsed_lines.append(line)
                elif (parsing and re.search(r':\w+-\w+-\w+-\w+-\w+#|:\w+-\w+-\w+-\w+#', line)): 
                    parsing = False
                    parsed_lines.append(line)
                    parsed_paragraph.append(parsed_lines)
                    parsed_lines = []
                elif parsing:
                    parsed_lines.append(line)
    
        ne_mac = []
        for mm in parsed_paragraph:
            parsing = False
            header_skip = False
            macs = []
            router = ''
            for qq in mm:
                if '---' in qq and parsing == False and header_skip == False:
                    header_skip = True
                    continue
                elif re.search(r':\w+-\w+-\w+-\w+-\w+#|:\w+-\w+-\w+-\w+#', qq):
                    match = re.search(r':\w+-\w+-\w+-\w+-\w+#|:\w+-\w+-\w+-\w+#', qq)
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
        df2 = pd.DataFrame(data, columns=['HOSTNAME','Address','Age','Hardware Addr','State','Type','Interface'])
    else:
        columns = ['HOSTNAME','Address','Age','Hardware Addr','State','Type','Interface']
        df2 = pd.DataFrame(columns=columns)

    return df1, df2
