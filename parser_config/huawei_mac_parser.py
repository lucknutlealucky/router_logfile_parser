import pandas as pd
import re
import sys
import fnmatch

def parse_mac_info(papayukero):
    parsing = False
    parsed_paragraph = []
    parsed_lines = []
    
    for line in papayukero:
        if 'display mac-address' in line:
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
        ne_mac = []
        # if len(parsed_paragraph) > 0:
        #     print('got hit mac-address')

        for mm in parsed_paragraph:
            parsing = False
            header_skip = False
            macs = []
            router = ''
            for qq in mm:
                if '---' in qq and parsing == False and header_skip == False:
                    
                    header_skip = True
                    continue
                elif re.search(r'<(.*?)>', qq):
                    match = re.search(r'<(.*?)>', qq)
                    if match:
                        router = match.group().replace('<','').replace('>','')

                elif '---' in qq and parsing == False and header_skip == True:
                    header_skip = False
                    parsing = True
                    continue
                    
                elif '---' in qq and parsing == True and header_skip == False:
                    parsing = False
                    continue
                    
                elif parsing and (header_skip == False):
                    used = qq.replace('\n','')

                    usedd = used.split()

                    stopper = len(used)

                    try:
                        if len(usedd) == 7:
                            macs.append([usedd[0],usedd[1],usedd[2],usedd[3],usedd[4],usedd[5],usedd[6]])
                        # elif len(usedd) == 3:
                        #     macs.append(['',
                        #             '',
                        #             '',
                        #             '',
                        #             ''.join(used[41:57]).replace(' ',''),
                        #             ''.join(used[57:67]).replace(' ',''),
                        #             ''.join(used[67:stopper]).replace(' ','')])
                        else:
                            macs.append([''.join(used[0:15]).replace(' ',''),
                                    ''.join(used[15:47]).replace(' ',''),
                                    ''.join(used[47:54]).replace(' ',''),
                                    ''.join(used[54:61]).replace(' ',''),
                                    ''.join(used[61:96]).replace(' ',''),
                                    ''.join(used[96:106]).replace(' ',''),
                                    ''.join(used[106:stopper]).replace(' ','')])

                    except:
                        macs.append([''.join(used[0:15]).replace(' ',''),
                                    ''.join(used[15:47]).replace(' ',''),
                                    ''.join(used[47:54]).replace(' ',''),
                                    ''.join(used[54:61]).replace(' ',''),
                                    ''.join(used[61:96]).replace(' ',''),
                                    ''.join(used[96:106]).replace(' ',''),
                                    ''.join(used[106:stopper]).replace(' ','')])

            for uu in macs:
                ne_mac.append([router,uu[0],uu[1],uu[2],uu[3],uu[4],uu[5],uu[6]])
                # if router == 'MDN-BLAT-EN1-910C-OPT':
                #     ne_mac.append([papayukero,uu[0],uu[1],uu[2],uu[3],uu[4],uu[5],uu[6]])
                # else:
                #     ne_mac.append([router,uu[0],uu[1],uu[2],uu[3],uu[4],uu[5],uu[6]])

        ne_mac_df = pd.DataFrame(ne_mac, columns=['HOSTNAME', 'MAC_Address', 'VSI', 'PEVLAN', 'CEVLAN', 'Port/Peerip', 'Type', 'LSP/LSR-ID'])
        
        for index, row in ne_mac_df.iterrows():
            if pd.isna(row['MAC_Address']) or row['MAC_Address'] == '':
                if index > 0:
                    ne_mac_df.at[index, 'MAC_Address'] = ne_mac_df.at[index - 1, 'MAC_Address']
            if pd.isna(row['VSI']) or row['VSI'] == '':
                if index > 0:
                    ne_mac_df.at[index, 'VSI'] = ne_mac_df.at[index - 1, 'VSI']
            if pd.isna(row['PEVLAN']) or row['PEVLAN'] == '':
                if index > 0:
                    ne_mac_df.at[index, 'PEVLAN'] = ne_mac_df.at[index - 1, 'PEVLAN']
            if pd.isna(row['CEVLAN']) or row['CEVLAN'] == '':
                if index > 0:
                    ne_mac_df.at[index, 'CEVLAN'] = ne_mac_df.at[index - 1, 'CEVLAN']

        # ne_mac_df['keys'] = ne_mac_df['HOSTNAME'] + "_" + ne_mac_df['Port/Peerip']

        # out_df = ne_mac_df.merge(ne_interface_df[['flags','keys']], on='keys', how='left')

        # out_df_save = out_df[~(out_df['flags'] == True) & ~out_df['Port/Peerip'].str.startswith(('Global', 'VE', 'Virtual')) & ~out_df['Port/Peerip'].str.match(r'\b(?:\d{1,3}\.){3}\d{1,3}\b')]
        
        out_df_save = ne_mac_df[['HOSTNAME','MAC_Address', 'VSI', 'PEVLAN', 'CEVLAN', 'Port/Peerip', 'Type', 'LSP/LSR-ID']]

        return out_df_save
    else:
        columns = ['HOSTNAME','MAC_Address', 'VSI', 'PEVLAN', 'CEVLAN', 'Port/Peerip', 'Type', 'LSP/LSR-ID']
        df = pd.DataFrame(columns=columns)
        return df