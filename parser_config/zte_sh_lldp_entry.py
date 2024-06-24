import re
import pandas as pd
import numpy as np

def zte_sh_lldp_entry(papayukero,ff):
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
            if 'show lldp entry' in line:
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
                for line in papa:
                    if first and '---' in line:
                        header_found = True
                        first = False
                        continue
                    elif header_found and ('---' in line or (re.search(r'^[A-Za-z0-9-]*#', line))): 
                        dferr.append(' '.join(filtered_data))
                        filtered_data = []
                        continue
                    elif header_found:
                        filtered_data.append(line)

            if len(dferr) > 0:
                powpow = []
                for i in dferr:
                    patterns = {
                        'Local_port': r'Local port\s*:\s*(.*)',
                        'Group_MAC_address': r'Group MAC address\s*:\s*(.*)',
                        'Neighbor_index': r'Neighbor index\s*:\s*(.*)',
                        'Chassis_type': r'Chassis type\s*:\s*(.*)',
                        'Chassis_ID': r'Chassis ID\s*:\s*(.*)',
                        'Port_ID_type': r'Port ID type\s*:\s*(.*)',
                        'Port_ID': r'Port ID\s*:\s*(.*)',
                        'Time_to_live': r'Time to live\s*:\s*(.*)',
                        'Port_description': r'Port description\s*:\s*(.*)',
                        'System_name': r'System name\s*:\s*(.*)',
                        'System_description': r'System description\s*:\s*([\s\S]*?)\n\s*System capabilities',
                        'System_capabilities': r'System capabilities\s*:\s*(.*)',
                        'Management_address': r'Management address\s*:\s*(.*)',
                        'Port_VLAN_ID(PVID)': r'Port VLAN ID\(PVID\)\s*:\s*(.*)',
                        'Aggregation_capability': r'Aggregation capability\s*:\s*(.*)',
                        'Aggregation_status': r'Aggregation status\s*:\s*(.*)',
                        'Aggregation_port ID': r'Aggregation port ID\s*:\s*(.*)',
                    }
                    
                    # Initialize a dictionary to store the parsed data
                    parsed_data = {key: '' for key in patterns}
                    
                    # Extract data using regex
                    for key, pattern in patterns.items():
                        match = re.search(pattern, i, re.MULTILINE)
                        if match:
                            parsed_data[key] = match.group(1).strip().replace('  ','').replace('\n','')
                    
                    # Create a DataFrame
                    df = pd.DataFrame([parsed_data])
                    
                    # Display the DataFrame
                    powpow.append(df)
                return pd.concat(powpow)
            else:
                return pd.DataFrame(columns=['Local_port', 'Group_MAC_address', 'Neighbor_index', 'Chassis_type',
                                               'Chassis_ID', 'Port_ID_type', 'Port_ID', 'Time_to_live',
                                               'Port_description', 'System_name', 'System_description',
                                               'System_capabilities', 'Management_address', 'Port_VLAN_ID(PVID)',
                                               'Aggregation_capability', 'Aggregation_status', 'Aggregation_port_ID'])

        else:
            return pd.DataFrame(columns=['Local_port', 'Group_MAC_address', 'Neighbor_index', 'Chassis_type',
                                               'Chassis_ID', 'Port_ID_type', 'Port_ID', 'Time_to_live',
                                               'Port_description', 'System_name', 'System_description',
                                               'System_capabilities', 'Management_address', 'Port_VLAN_ID(PVID)',
                                               'Aggregation_capability', 'Aggregation_status', 'Aggregation_port_ID'])
                                    
    except Exception as e:
        print(ff)
        print(parsed_paragraph)
        # print(ii)
        # print(mm)
        # print(i)
        # print(e)
        raise
            