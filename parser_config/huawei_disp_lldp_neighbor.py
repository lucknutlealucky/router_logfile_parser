import pandas as pd
import re


def hw_disp_lldp_neighbor(papayukeri,ff):

    pewpewpew = []

    for i in papayukeri:
        pewpewpew.append(i)

    liner = pewpewpew
    # print(liner)
    long_line = len(pewpewpew)
    pattern = r'(\w+/\d+/\d+) has (\d+) neighbor\(s\):'
    parsing = False
    parsed_lines = []
    special_lines = []
    parsed_paragraph = []
    hostname = ''
    get_sys_description = False
    ndasmu = ''
    many_neighbor = False

    for index, line in enumerate(liner):
        match = re.search(pattern, line)
        
        if match and get_sys_description == False and many_neighbor == False:
            parsing = True
            if int(match.group(2)) > 1:
                ndasmu = line
                many_neighbor = True
            else:
                parsed_lines.append(line)

        elif many_neighbor == True and 'Neighbor index' in line and parsing == False:
            parsing = True
            parsed_lines.append(ndasmu)
            parsed_lines.append(line)

        elif many_neighbor == True and 'Neighbor index' in liner[index + 1] and parsing == True:
            if len(parsed_lines) > 0:
                parsed_paragraph.append(parsed_lines)
                # print('inselt')
            parsed_lines = []
            parsing = False
            
        elif get_sys_description == False and parsing and 'Port description' in line:
            parsed_lines.append('###' +' '.join(line.split()).strip()+'###')
            
        elif get_sys_description == False and 'System description' in line:
            get_sys_description = True
            special_lines.append(' '.join(line.split()).strip())
            
        elif get_sys_description == True and 'System capabilities supported' in line:
            get_sys_description = False
            parsed_lines.append('####' + ' '.join(special_lines).strip().replace('\n','') + '####\n')
            parsed_lines.append(line)
            special_lines = []
            
        elif parsing and (re.search(r'<(.*?)>', line) or 'has 0 neighbor(s)' in line or re.search(pattern,liner[index + 1])):
            parsed_paragraph.append(parsed_lines)
            # print('inselst')
            parsed_lines = []
            parsing = False
            many_neighbor = False
            
        elif parsing and get_sys_description==False:
            parsed_lines.append(line)
            
        elif parsing and get_sys_description==True:
            special_lines.append(line)


        if (re.search(r'<(.*?)>', line) and long_line > (index + 1)): 
            if '=' in liner[index + 1]:
                hostname = line.replace('<', '').replace('>', '').replace('\n','')
        
        if (re.search(r'<(.*?)>', line) and long_line == (index + 1)):
            hostname = line.replace('<', '').replace('>', '').replace('\n','')
        
        
    

    if len(parsed_paragraph) > 0: 
        patternsz = {
            'Host_Name' : '',
            'Interface' : r'(\w+/\d+/\d+) has (\d+) neighbor\(s\):',
            'Neighbor index': r'Neighbor index\s*:\s*(\d+)',
            'Chassis type': r'Chassis type\s*:\s*([\w-]+)',
            'Chassis ID': r'Chassis ID\s*:\s*([\da-f-]+)',
            'Port ID type': r'Port ID type\s*:\s*([\w-]+)',
            'Port ID': r'Port ID\s*:\s*([\w/-]+)',
            'Port description': r'\s*###\s*Port description\s*\s*:\s*(.+)\s*###\s*',
            'System name': r'System name\s*:\s*([\w-]+)',
            'System description': r'####System description\s*:\s*(.+)####',
            'System capabilities supported': r'System capabilities supported\s*:\s*([\w\s]+)',
            'System capabilities enabled': r'System capabilities enabled\s*:\s*([\w\s]+)',
            'Management address type': r'Management address type\s*:\s*([\w\d]+)',
            'Management address': r'Management address\s*:\s*([\d.]+)',
            'Expired time': r'Expired time\s*:\s*(\d+s)',
            'Port VLAN ID (PVID)': r'Port VLAN ID\(PVID\)\s*:\s*(\d+)',
            'Port and Protocol VLAN ID (PPVID)': r'Port and Protocol VLAN ID\(PPVID\)\s*:\s*([\w-]+)',
            'Port and Protocol VLAN supported': r'Port and Protocol VLAN supported\s*:\s*([\w-]+)',
            'Port and Protocol VLAN enabled': r'Port and Protocol VLAN enabled\s*:\s*([\w-]+)',
            'VLAN name of VLAN': r'VLAN name of VLAN\s*:\s*([\w-]+)',
            'Protocol identity': r'Protocol identity\s*:\s*([\w-]+)',
            'Auto-negotiation supported': r'Auto-negotiation supported\s*:\s*([\w-]+)',
            'Auto-negotiation enabled': r'Auto-negotiation enabled\s*:\s*([\w-]+)',
            'OperMau (speed / duplex)': r'OperMau\s*:\s*speed\s*\(([\w\d-]*)\)\s*/\s*duplex\s*\(([\w\d-]*)\)',
            'Link aggregation supported': r'Link aggregation supported\s*:\s*([\w-]+)',
            'Link aggregation enabled': r'Link aggregation enabled\s*:\s*([\w-]+)',
            'Aggregation port ID': r'Aggregation port ID\s*:\s*(\d+)',
            'Maximum frame Size': r'Maximum frame Size\s*:\s*(\d*)',
            'Discovered time': r'Discovered time\s*:\s*([\d-]+\s*[\d:]+\+\d{2}:\d{2})',
            'Unrecognized organizationally defined TLV TLV OUI': r' TLV OUI\s*:\s*([\da-f-]+)',
            'Unrecognized organizationally defined TLV TLV subtype': r' TLV subtype\s*:\s*(\d+)',
            'Unrecognized organizationally defined TLV Index': r' Index\s*:\s*(\d+)',
            'Unrecognized organizationally defined TLV TLV information': r' TLV information\s*:\s*([\d\w]+)',
        }


        insider = []
        for pp in parsed_paragraph:
            text = ' '.join(pp)
            data = {kpi: '' for kpi in patternsz}

            for line in text.split('\n'):
                for kpi, patternv in patternsz.items():
                    if kpi == 'Host_Name':
                            data[kpi] = hostname
                    else:
                        match = re.search(patternv, line)
                        if match:
                            if kpi == 'Interface':
                                data[kpi] = match.group(1).strip()

                            else:
                                data[kpi] = match.group(1).strip() if len(match.groups()) == 1 else " / ".join(match.groups()).strip()

            # print(data)           
            insider.append(pd.DataFrame([data]))

        # print(hostname)

        # print(len(parsed_paragraph))
        # print(parsed_paragraph)

        df = pd.concat(insider,ignore_index=True)
        # print(df.head())

        return df[['Host_Name', 'Interface', 'Neighbor index', 'Chassis type',
                    'Chassis ID', 'Port ID type', 'Port ID', 'Port description',
                    'System name', 'System description', 'System capabilities supported',
                    'System capabilities enabled', 'Management address type',
                    'Management address', 'Expired time', 'Port VLAN ID (PVID)',
                    'Port and Protocol VLAN ID (PPVID)', 'Port and Protocol VLAN supported',
                    'Port and Protocol VLAN enabled', 'VLAN name of VLAN',
                    'Protocol identity', 'Auto-negotiation supported',
                    'Auto-negotiation enabled', 'OperMau (speed / duplex)',
                    'Link aggregation supported', 'Link aggregation enabled',
                    'Aggregation port ID', 'Maximum frame Size', 'Discovered time',
                    'Unrecognized organizationally defined TLV TLV OUI',
                    'Unrecognized organizationally defined TLV TLV subtype',
                    'Unrecognized organizationally defined TLV Index',
                    'Unrecognized organizationally defined TLV TLV information']]

    else:
        columnsc = ['Host_Name', 'Interface', 'Neighbor index', 'Chassis type',
                    'Chassis ID', 'Port ID type', 'Port ID', 'Port description',
                    'System name', 'System description', 'System capabilities supported',
                    'System capabilities enabled', 'Management address type',
                    'Management address', 'Expired time', 'Port VLAN ID (PVID)',
                    'Port and Protocol VLAN ID (PPVID)', 'Port and Protocol VLAN supported',
                    'Port and Protocol VLAN enabled', 'VLAN name of VLAN',
                    'Protocol identity', 'Auto-negotiation supported',
                    'Auto-negotiation enabled', 'OperMau (speed / duplex)',
                    'Link aggregation supported', 'Link aggregation enabled',
                    'Aggregation port ID', 'Maximum frame Size', 'Discovered time',
                    'Unrecognized organizationally defined TLV TLV OUI',
                    'Unrecognized organizationally defined TLV TLV subtype',
                    'Unrecognized organizationally defined TLV Index',
                    'Unrecognized organizationally defined TLV TLV information']

        return pd.DataFrame(columns=columnsc)