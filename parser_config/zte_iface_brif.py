import re
import pandas as pd
import numpy as np

def get_patern(log_text):
    if "=" in log_text:
        # print('special')
        # Define regular expressions to extract information
        ip_pattern = re.compile(r'who@(\d+\.\d+\.\d+\.\d+)')
        hostname_pattern = re.compile(r'Hostname\s*:\s*([\w-]+)')
        device_pattern = re.compile(r'Device\s*:\s*([\w\d-]+)')
        site_id_pattern = re.compile(r'Site_ID\s*:\s*([\w\d]+)')
        site_name_pattern = re.compile(r'Site_Name\s*:\s*([\w\d_-]+)')
        address_pattern = re.compile(r'Address\s*:\s*([^=]+)')
        city_name_pattern = re.compile(r'City_Name\s*:\s*([\w-]+)')
        region_pattern = re.compile(r'Region\s*:\s*([\w-]+)')
        latitude_pattern = re.compile(r'Latitude\s*:\s*([\d.,]+)')
        longitude_pattern = re.compile(r'Longitude\s*:\s*([\d.,]+)')
        branch_name_pattern = re.compile(r'Branch_Name\s*:\s*([\w-]+)')
        contact_pattern = re.compile(r'Contact\s*:\s*([\w.@]+)')
        
        # Extract information using regular expressions
        ip_match = ip_pattern.search(log_text)
        hostname_match = hostname_pattern.search(log_text)
        device_match = device_pattern.search(log_text)
        site_id_match = site_id_pattern.search(log_text)
        site_name_match = site_name_pattern.search(log_text)
        address_match = address_pattern.search(log_text)
        city_name_match = city_name_pattern.search(log_text)
        region_match = region_pattern.search(log_text)
        latitude_match = latitude_pattern.search(log_text)
        longitude_match = longitude_pattern.search(log_text)
        branch_name_match = branch_name_pattern.search(log_text)
        contact_match = contact_pattern.search(log_text)
        
        # Display the extracted information
        if ip_match:
            ip_address = ip_match.group(1)
            
        else:
            ip_address = ''
        
        if hostname_match:
            hostname = hostname_match.group(1)
            
        else:
            hostname = ''
        
        if device_match:
            device = device_match.group(1)
            
        else:
            device = ''
        
        if site_id_match:
            site_id = site_id_match.group(1)
            
        else:
            site_id = ''
        
        if site_name_match:
            site_name = site_name_match.group(1)
            
        else:
            site_name = ''
        
        if address_match:
            address = address_match.group(1).strip()
            
        else:
            address = ''
        
        if city_name_match:
            location = city_name_match.group(1)
            
        else:
            location = ''
        
        if region_match:
            region = region_match.group(1)
            
        else:
            region = ''
        
        if latitude_match:
            latitude = latitude_match.group(1)
            
        else:
            latitude = ''
        
        if longitude_match:
            longitude = longitude_match.group(1)
            
        else:
            longitude = ''
    else:
        ip_pattern = re.compile(r'who@(\d+\.\d+\.\d+\.\d+)')
        site_name_pattern = re.compile(r'Site Name // Hostname\s*:\s*([\w-]+)\s*//\s*([\w-]+)')
        site_id_pattern = re.compile(r'Site ID // Device\s*:\s*([\w\d]+)\s*//\s*([\w\d-]+)')
        longitude_latitude_pattern = re.compile(r'Longitude // Latitude\s*:\s*(-?\d+\.\d+) // (-?\d+\.\d+)')
        address_pattern = re.compile(r'Address\s*:\s*([^*]+)')
        location_pattern = re.compile(r'Location\s*:\s*([^*]+)')
        
        # Extract information using regular expressions
        ip_match = ip_pattern.search(log_text)
        site_name_match = site_name_pattern.search(log_text)
        site_id_match = site_id_pattern.search(log_text)
        longitude_latitude_match = longitude_latitude_pattern.search(log_text)
        address_match = address_pattern.search(log_text)
        location_match = location_pattern.search(log_text)
        
        
        if ip_match:
            ip_address = ip_match.group(1)
            
        else:
            ip_address = ''
        
        if site_name_match:
            site_name = site_name_match.group(1)
            hostname = site_name_match.group(2)
            
        else:
            site_name = ''
            hostname = ''
        
        if site_id_match:
            site_id = site_id_match.group(1)
            device = site_id_match.group(2)
            
        else:
            site_id = ''
            device = ''
        
        if longitude_latitude_match:
            longitude = longitude_latitude_match.group(1)
            latitude = longitude_latitude_match.group(2)
            
        else:
            longitude = ''
            latitude = ''
        
        if address_match:
            address = address_match.group(1).strip()
        else:
            address = ''
        
        if location_match:
            location = location_match.group(1).strip()

        else:
            location = ''
    return [ip_address, site_name, hostname, site_id, device, longitude, latitude, address, location]

def parse_log_file(log_file_path):
    parsing = False
    parsing2 = False
    parsed_paragraph = []
    parsed_lines = []
    router_lines = []
    router_graph = []
    to_output = []
    log_content = []

    with open(log_file_path, 'r') as file:
        for line in file:
            if 'Login at' in line:
                parsing = True
                parsed_lines.append(line)
            elif (parsing and re.search(r'([A-Z0-9-]+)#exit', line)) or (parsing and 'Write failed: Broken pipe' in line):
                parsing = False
                parsed_lines.append(line)
                parsed_paragraph.append(parsed_lines)
                parsed_lines = []
            elif parsing:
                parsed_lines.append(line)

            password_match = re.search(r'who@(\d+\.\d+\.\d+\.\d+\'s password:)', line)
            if (password_match and (parsing2 == False)) and (len(router_lines) == 0):
                router_lines.append('who@' + password_match.group(1))
                
            if 'You are entering PT INDOSAT' in line:
                parsing2 = True
                router_lines.append(line)
            elif parsing2 and ('Login at' in line):
                parsing2 = False
                router_lines.append(line)
                router_graph.append('\n'.join(router_lines))
                router_lines = []
            elif parsing2:
                router_lines.append(line)

    head_parsed = router_graph
    mm = 0 
    regex = re.compile(r'([A-Z0-9-]+)#term length')
    for m in parsed_paragraph:
        ne = ''
        for i in m:     
            match = regex.search(i)
            if match:
                extracted_part = match.group(1)

        parsed_line_2nd = []

        parsing = False
        for line in m:
            if 'show interface brief' in line:
                parsing = True
                # parsed_line_2nd.append(line)

            elif (parsing and re.search(r'([A-Z0-9-]+)#exit', line)) or (parsing and 'Write failed: Broken pipe' in line):
                parsing = False
                # parsed_line_2nd.append(line)
                break
            elif parsing:
                parsed_line_2nd.append(line)

        to_output.append([head_parsed[mm],extracted_part,parsed_line_2nd])
        mm = mm + 1

    return to_output

def zte_iface_mapper(log_file_path):

    result_list = []
    # log_file_path = '3k_map_2_zte_log_20240201.log'
    resultss = parse_log_file(log_file_path)

    # print(resultss[1])
    for ppp in resultss:
        kepala = get_patern(ppp[0])
        
        # config_text = ''.join(ppp[2])
        data = []
        
        for line in ppp[2]:
            if "Interface" in line:  # Skip header line
                continue
                
            if "Description" in line:
                continue
                
            parts = line.split()

            if len(parts) > 0 and len(parts) < 6:
                if not str(parts[0]).replace(" ", "") == '':
                    interface_data = {
                        "IP Address": kepala[0], 
                        "Site Name": kepala[1],
                        "Hostname": kepala[2],
                        "Site ID": kepala[3],
                        "Device": kepala[4],
                        "Longitude": kepala[5],
                        "Latitude": kepala[6],
                        "Address": kepala[7],
                        "Location": kepala[8],
                        "Host NE": ppp[1],
                        "Interface": '',
                        "Attribute": '',
                        "Mode": '',
                        "BW": '',
                        "Admin": '',
                        "Phy": '',
                        "Prot": '',
                        "Description": " ".join(parts)
                    }
                    data.append(interface_data)
            
            elif len(parts) >= 6:
                if len(parts) == 6 and (str(parts[3]).upper() == 'UP' or str(parts[3]).upper() == 'DOWN') and (('OPTICAL' in str(parts[1]).upper()) or ('ELECTRIC' in str(parts[1]).upper()) or ('N/A' in str(parts[1]).upper())):
                    interface_data = {
                        "IP Address": kepala[0], 
                        "Site Name": kepala[1],
                        "Hostname": kepala[2],
                        "Site ID": kepala[3],
                        "Device": kepala[4],
                        "Longitude": kepala[5],
                        "Latitude": kepala[6],
                        "Address": kepala[7],
                        "Location": kepala[8],
                        "Host NE": ppp[1],
                        "Interface": parts[0],
                        "Attribute": parts[1],
                        "Mode": parts[2],
                        "BW": '',
                        "Admin": parts[3],
                        "Phy": parts[4],
                        "Prot": parts[5],
                        "Description": ''
                    }
                    data.append(interface_data)

                    # print(interface_data)
                    
                elif len(parts) == 7 and not (str(parts[3]).upper() == 'UP' or str(parts[3]).upper() == 'DOWN') and (('OPTICAL' in str(parts[1]).upper()) or ('ELECTRIC' in str(parts[1]).upper()) or ('N/A' in str(parts[1]).upper())):
                    interface_data = {
                        "IP Address": kepala[0], 
                        "Site Name": kepala[1],
                        "Hostname": kepala[2],
                        "Site ID": kepala[3],
                        "Device": kepala[4],
                        "Longitude": kepala[5],
                        "Latitude": kepala[6],
                        "Address": kepala[7],
                        "Location": kepala[8],
                        "Host NE": ppp[1],
                        "Interface": parts[0],
                        "Attribute": parts[1],
                        "Mode": parts[2],
                        "BW": parts[3],
                        "Admin": parts[4],
                        "Phy": parts[5],
                        "Prot": parts[6],
                        "Description": ""
                    }
                    data.append(interface_data)

                elif len(parts) >= 7 and (str(parts[3]).upper() == 'UP' or str(parts[3]).upper() == 'DOWN'):
                    interface_data = {
                        "IP Address": kepala[0], 
                        "Site Name": kepala[1],
                        "Hostname": kepala[2],
                        "Site ID": kepala[3],
                        "Device": kepala[4],
                        "Longitude": kepala[5],
                        "Latitude": kepala[6],
                        "Address": kepala[7],
                        "Location": kepala[8],
                        "Host NE": ppp[1],
                        "Interface": parts[0],
                        "Attribute": parts[1],
                        "Mode": parts[2],
                        "BW": '',
                        "Admin": parts[3],
                        "Phy": parts[4],
                        "Prot": parts[5],
                        "Description": " ".join(parts[6:])
                    }
                    data.append(interface_data)
                    
                elif len(parts) > 7 and not (str(parts[3]).upper() == 'UP' or str(parts[3]).upper() == 'DOWN') and (('OPTICAL' in str(parts[1]).upper()) or ('ELECTRIC' in str(parts[1]).upper()) or ('N/A' in str(parts[1]).upper())):
                    interface_data = {
                        "IP Address": kepala[0], 
                        "Site Name": kepala[1],
                        "Hostname": kepala[2],
                        "Site ID": kepala[3],
                        "Device": kepala[4],
                        "Longitude": kepala[5],
                        "Latitude": kepala[6],
                        "Address": kepala[7],
                        "Location": kepala[8],
                        "Host NE": ppp[1],
                        "Interface": parts[0],
                        "Attribute": parts[1],
                        "Mode": parts[2],
                        "BW": parts[3],
                        "Admin": parts[4],
                        "Phy": parts[5],
                        "Prot": parts[6],
                        "Description": " ".join(parts[8:])
                    }
                    data.append(interface_data)
                elif not (('INDONESIA' in str(parts[1]).upper()) or ('OPTICAL' in str(parts[1]).upper()) or ('ELECTRIC' in str(parts[1]).upper()) or ('N/A' in str(parts[1]).upper())):
                    interface_data = {
                        "IP Address": kepala[0], 
                        "Site Name": kepala[1],
                        "Hostname": kepala[2],
                        "Site ID": kepala[3],
                        "Device": kepala[4],
                        "Longitude": kepala[5],
                        "Latitude": kepala[6],
                        "Address": kepala[7],
                        "Location": kepala[8],
                        "Host NE": ppp[1],
                        "Interface": "",
                        "Attribute": "",
                        "Mode": "",
                        "BW": "",
                        "Admin": "",
                        "Phy": "",
                        "Prot": "",
                        "Description": " ".join(parts)
                    }
                    data.append(interface_data)
                    
            
                
        result_list.append(pd.DataFrame(data)) 
        
            
    df = pd.concat(result_list).reset_index(drop=True)

    for index, row in df.iterrows():
        if pd.isna(row['Prot']) or row['Prot'] == '':
            if index > 0:
                df.at[index - 1, 'Description'] = df.at[index, 'Description']
        
    df = df.replace("",np.nan)
    df = df.dropna(subset=['Interface','Attribute','Mode','BW','Admin','Phy','Prot'],how='all').reset_index(drop=True)
    
    # Apply the function to all elements in the DataFrame
    # df_cleaned = df.replace({r'\n': ''}, regex=True).replace({r'\r': ''}, regex=True).replace(r';', '', regex=True)

    return df