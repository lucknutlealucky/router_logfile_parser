import pandas as pd
import re
import os
import fnmatch
import numpy as np

def hw_get_iface_info(papayukero):
    parsing = False
    parsed_paragraph_x = []
    parsed_lines = []
    
    # print('parsing peer info', papayukero)
    
    for line in papayukero:
        if 'display interface brief' in line and not('main' in line):
            parsing = True
            parsed_lines.append(line)
        elif (parsing and re.search(r'<(.*?)>', line)): 
            parsing = False
            parsed_lines.append(line)
            parsed_paragraph_x.append(parsed_lines)
            parsed_lines = []
        elif parsing:
            parsed_lines.append(line)

    parsing = False
    parsed_paragraph = []
    parsed_lines = []
    for qq in parsed_paragraph_x:
        for pp in qq:
            if 'InUti/OutUti: input utility/output utility' in pp:
                # print('aw')
                parsing = True
                parsed_lines.append(pp)
            elif (parsing and re.search(r'<(.*?)>', pp)): 
                parsing = False
                parsed_lines.append(pp)
                parsed_paragraph.append(parsed_lines)
                parsed_lines = []
            elif parsing:
                parsed_lines.append(pp)
    
    dfeer = []
    for data in parsed_paragraph:
        parsed_data = []
        parent_interface = None
        for line in data[2:]:
            if line.strip() == "":
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            if '-Trunk' in parts[0]: 
                parent_interface = parts[0]
            else:
                if line.startswith('  '): 
                    interface = str(parent_interface) + "|" + parts[0]
                else:
                    interface = parts[0]
                phy = parts[1]
                protocol = parts[2]
                in_utility = parts[3]
                out_utility = parts[4]
                inErrors = parts[5]
                outErrors = parts[6]
                host_name = data[-1].strip().replace('<','').replace('>','')
                parsed_data.append({
                    'host_name': host_name,
                    'Interface': interface,
                    'PHY': phy,
                    'Protocol': protocol,
                    'InUti': in_utility,
                    'OutUti': out_utility,
                    'inErrors': inErrors,
                    'outErrors': outErrors
                })
                
        df = pd.DataFrame(parsed_data)
        dfeer.append(df)

    try:
        return pd.concat(dfeer)
    except:
        return pd.DataFrame(columns=['host_name', 'Interface', 'PHY', 'Protocol', 'InUti', 'OutUti', 'inErrors', 'outErrors'])


def hw_get_iface_info_main(papayukero):
    parsing = False
    parsed_paragraph_x = []
    parsed_lines = []
    
    # print('parsing peer info', papayukero)
    
    for line in papayukero:
        if 'display interface brief main' in line:
            parsing = True
            parsed_lines.append(line)
        elif (parsing and re.search(r'<(.*?)>', line)): 
            parsing = False
            parsed_lines.append(line)
            parsed_paragraph_x.append(parsed_lines)
            parsed_lines = []
        elif parsing:
            parsed_lines.append(line)

    parsing = False
    parsed_paragraph = []
    parsed_lines = []
    for qq in parsed_paragraph_x:
        for pp in qq:
            if 'InUti/OutUti: input utility/output utility' in pp:
                # print('aw')
                parsing = True
                parsed_lines.append(pp)
            elif (parsing and re.search(r'<(.*?)>', pp)): 
                parsing = False
                parsed_lines.append(pp)
                parsed_paragraph.append(parsed_lines)
                parsed_lines = []
            elif parsing:
                parsed_lines.append(pp)
    
    dfeer = []
    for data in parsed_paragraph:
        parsed_data = []
        parent_interface = None
        for line in data[2:]:
            if line.strip() == "":
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            if '-Trunk' in parts[0]: 
                parent_interface = parts[0]
            else:
                if line.startswith('  '): 
                    interface = str(parent_interface) + "|" + parts[0]
                else:
                    interface = parts[0]
                phy = parts[1]
                protocol = parts[2]
                in_utility = parts[3]
                out_utility = parts[4]
                inErrors = parts[5]
                outErrors = parts[6]
                host_name = data[-1].strip().replace('<','').replace('>','')
                parsed_data.append({
                    'host_name': host_name,
                    'Interface': interface,
                    'PHY': phy,
                    'Protocol': protocol,
                    'InUti': in_utility,
                    'OutUti': out_utility,
                    'inErrors': inErrors,
                    'outErrors': outErrors
                })
                
        df = pd.DataFrame(parsed_data)
        dfeer.append(df)

    try:
        return pd.concat(dfeer)
    except:
        return pd.DataFrame(columns=['host_name', 'Interface', 'PHY', 'Protocol', 'InUti', 'OutUti', 'inErrors', 'outErrors'])