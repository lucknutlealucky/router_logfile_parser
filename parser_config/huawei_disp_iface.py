import re
import pandas as pd
import os
import fnmatch
import numpy as np

def remove_empty_pairs(data):
    filtered_data = []
    for item in data:
        # Filter out the empty string pairs
        filtered_item = tuple(subitem for subitem in item if subitem)
        filtered_data.append(filtered_item)
    return filtered_data

def get_hw_iface(papayukero, file_namessss):
    try:
        parsing = False
        parsed_paragraphx = []
        parsed_lines = []
        host_name = ''
        
        poki = []
        for line in papayukero:
            poki.append(line)
        try:
            for index, line in enumerate(poki):
                if 'display interface' in line and not('brief' in line):
                    parsing = True
                    parsed_lines.append(line)
                elif (parsing and re.search(r'<(.*?)>', line) and '=' in poki[index + 1]): 
                    host_name = line.replace('<', '').replace('>', '').replace('\n','')
                    parsing = False
                    parsed_lines.append(line)
                    parsed_paragraphx.append(parsed_lines)
                    parsed_lines = []
                elif parsing:
                    parsed_lines.append(line)
        except:
            pass

        # print('hostname1', host_name)

        
        if len(parsed_paragraphx) > 0 and len(parsed_paragraphx[0]) > 2:
            
            # print(parsed_paragraphx)
            
            parsing = False
            take_flag = False
            parsed_paragraph = []
            parsed_lines = []
            pedeep = []

            for mm in parsed_paragraphx:
                for pp in mm:
                    if 'current state :' in pp:
                        parsing = True
                        parsed_lines.append(pp)
                    elif 'Rx Power' in pp or 'Rx Warning range' in pp:
                        take_flag = True
                        parsed_lines.append(pp)
                    elif ((pp == '\n' or pp == ' \n') or (pp == '  \n' or pp == '   \n')) or ((pp == '\n ' or pp == '\n  ') or (pp == '\n   ' or pp == ' \n ')): 
                        parsing = False
                        parsed_lines.append(pp)
                        if len(parsed_lines) > 5 and take_flag == True:
                            parsed_paragraph.append(parsed_lines)
                            take_flag = False
                        parsed_lines = []
                    elif parsing:
                        parsed_lines.append(pp)
                        
            
            if not len(parsed_paragraph) > 0:
                kolom1 = ['Host_Name','Interface','Line protocol current state','Link quality grade','Description','IP Sending Frames',
                            'Hardware address','Factor/Module','Standard','FecMode',
                            'The Vendor PN','The Vendor Name','Port BW','Transceiver max BW',
                            'Transceiver Mode','Connector Type','Transmission Distance','WaveLength',
                            'Current system time','Input','Output','Last line protocol up time','Maximal BW',
                            'Current BW','The Maximum Transmit Unit','Internet Address','Vlan ID',
                            'Last 300 seconds input utility rate','Last 300 seconds output utility rate',
                            'Last 10 seconds input utility rate','Last 10 seconds output utility rate']

                kolom2 = ["interface","description","port_bw","transceiver_max_bw",
                                "transceiver_mode","wavelength","transmission_distance","internet_address",
                                "rx_warning_range","tx_warning_range","transmitter_index","rx_power","tx_power"]

                return  pd.DataFrame(columns=kolom1), pd.DataFrame(columns=kolom2)

                        
            regex_patterns = {
                    'Line protocol current state': r'Line protocol current state : (\w+)',
                    'Link quality grade': r'Link quality grade : (\w+)',
                    'Description': r'Description: (.+)',
                    'IP Sending Frames': r"IP Sending Frames' Format is (\w+)",
                    'Hardware address': r'Hardware address is (\w+-\w+-\w+)',
                    'Factor/Module': r'Factor/Module: (\w+)',
                    'Standard': r'Standard: (\w+)',
                    'FecMode': r'FecMode: (\w+)',
                    'The Vendor PN': r'The Vendor PN is (\w+)',
                    'The Vendor Name': r'The Vendor Name is (\w+)',
                    'Port BW': r'Port BW: (\d+G)',
                    'Transceiver max BW': r'Transceiver max BW: (\d+\.\d+Gbps)',
                    'Transceiver Mode': r'Transceiver Mode: (\w+)',
                    'Connector Type': r'Connector Type: (\w+)',
                    'Transmission Distance': r'Transmission Distance: (\d+\w+)',
                    'WaveLength': r'WaveLength: (.+)',
                    'Current system time': r'Current system time: (.+)',
                    'Input': r'Input: ([\d\s\w.:+-]+Output:)',
                    'Output': r'Output: ([\d\s\w.:+-]+Local fault:)',
                    'Last line protocol up time': r'Last line protocol up time : (.+)',
                    'Maximal BW': r'Maximal BW: (\d+Gbps)',
                    'Current BW': r'Current BW: (\d+Gbps)',
                    'The Maximum Transmit Unit': r'The Maximum Transmit Unit is (\d+)',
                    'Internet Address': r'Internet Address is (.+)',
                    'Vlan ID': r'Vlan ID (\d+)',
                    'Input': r'Input: ([\d\s\w.,]+bytes)',
                    'Output': r'Output:([\d\s\w.,]+bytes)',
                    'Last 300 seconds input utility rate': r'Last 300 seconds input utility rate:  (\d+\.\d+%)',
                    'Last 300 seconds output utility rate': r'Last 300 seconds output utility rate: (\d+\.\d+%)',
                    'Last 10 seconds input utility rate': r'Last 10 seconds input utility rate:  (\d+\.\d+%+\n)',
                    'Last 10 seconds output utility rate': r'Last 10 seconds output utility rate: (\d+\.\d+%+\n)',
                }
            
            parsed_data_list = []
            for m in parsed_paragraph:
                iface = m[0].split()[0]
                data = ''.join(m)
                # print('hostname2', host_name)
                parsed_data = {'Interface': iface, 'Host_Name': host_name}
                
                
                for field, pattern in regex_patterns.items():
                    match = re.search(pattern, data)
                    if match:
                        parsed_data[field] = match.group(1).strip()
                    else:
                        parsed_data[field] = ''
            
                parsed_data_list.append(parsed_data)
            
            df = pd.DataFrame(parsed_data_list)
        
            columns_order = ['Host_Name','Interface','Line protocol current state','Link quality grade','Description','IP Sending Frames',
                            'Hardware address','Factor/Module','Standard','FecMode',
                            'The Vendor PN','The Vendor Name','Port BW','Transceiver max BW',
                            'Transceiver Mode','Connector Type','Transmission Distance','WaveLength',
                            'Current system time','Input','Output','Last line protocol up time','Maximal BW',
                            'Current BW','The Maximum Transmit Unit','Internet Address','Vlan ID',
                            'Last 300 seconds input utility rate','Last 300 seconds output utility rate',
                            'Last 10 seconds input utility rate','Last 10 seconds output utility rate']

            # print(df[['Host_Name']])
            
            df = df.reindex(columns=columns_order)

            pidiiiiiii = []

            for m in parsed_paragraph:
                interface = m[0].split()[0]
                data = ' '.join(m)

                # if data = None:
                #     continue

                description = re.search(r"Description: *(.*)", data).group(1).strip()

                if re.search(r"Port BW: ([^\s,]*)", data):
                    port_bw = re.search(r"Port BW: ([^\s,]*)", data).group(1)

                else :
                    port_bw = ''

                if re.search(r"Transceiver max BW: ([^\s,]*)", data):
                    transceiver_max_bw = re.search(r"Transceiver max BW: ([^\s,]*)", data).group(1)
                elif re.search(r"Transceiver max BW\(*M*B*i*t*\/*s*e*c*\)*: ([^\s,]*)", data):
                    transceiver_max_bw = re.search(r"Transceiver max BW\(*M*B*i*t*\/*s*e*c*\)*: ([^\s,]*)", data).group(1)
                else:
                    transceiver_max_bw = ''
                if re.search(r"Transceiver Mode: ([^\s,]*)", data):
                    transceiver_mode = re.search(r"Transceiver Mode: ([^\s,]*)", data).group(1)
                else:
                    transceiver_mode = ''
                wavelength = ''
                if re.search(r"(WaveLengh|WaveLength)\(*n*m*\)*: ([^\s]*)", data):
                    wavelength = re.search(r"(WaveLengh|WaveLength)\(*n*m*\)*: ([^\s]*)", data).group(2)
                transmission_distance = re.search(r"Transmission Distance\(*m*\)*: ([^\s]*\s*\w*\)*)", data).group(1)
                internet_address = re.search(r"Internet Address is ([^\s]*)", data)
                if internet_address:
                    internet_address = internet_address.group(1)

                

                rx_tx_powers_buf = re.findall(r"(Rx\d* Power: *-*\d+\.*\d+dBm|--dBm|--|dBm), *(Tx\d* Power: *-*\d+\.*\d+dBm|--dBm|--|dBm)|(Rx\d* Power\[.*?\]: *-*\d+\.*\d+dBm|--dBm|--|dBm), *(Tx\d* Power\[.*?\]: *-*\d+\.*\d+dBm|--dBm|--|dBm)", data)

                rx_tx_powers = remove_empty_pairs(rx_tx_powers_buf)
                
                if not rx_tx_powers:
                    rx_power = re.search(r"(Rx Power: ( *-*\d+\.*\d+dBm|--dBM|--|dBM))", data)
                    tx_power = re.search(r"(Tx Power: ( *-*\d+\.*\d+dBm|--dBM|--|dBM))", data)
                    if rx_power and tx_power:
                        rx_tx_powers = [(f"Rx0 Power: {rx_power.group(2)}", f"Tx0 Power: {tx_power.group(2)}")]
                    else:
                        pattern_rx_power = r'Current Rx Power\(dBM\)\s*:\s*(-?\d+\.\d+)'
                        pattern_tx_power = r'Current Tx Power\(dBM\)\s*:\s*(-?\d+\.\d+)'
                        Rx_Power = float(re.search(pattern_rx_power, data).group(1))
                        Tx_Power = float(re.search(pattern_tx_power, data).group(1))
                        if Rx_Power and Tx_Power:
                            rx_tx_powers = [(f"Rx0 Power: {Rx_Power}dBm", f"Tx0 Power: {Tx_Power}dBm")]


                rx_tx_warning_range = re.findall(r"(\w+ Warning range: (\[.*?\]dBm)), (\w+ Warning range: (\[.*?\]dBm))", data)

                

                
                
                # print(re.findall(r"(\w+ Warning range: (\[.*?\]dBm)), (\w+ Warning range: (\[.*?\]dBm))", data))
                # print(re.findall(r"((R\w+\d* Power: *-*\d+\.\d+dBm), (Warning range: (\[.*?\]dBm)))", data))
                # print(re.findall(r"((T\w+\d* Power: *-*\d+\.\d+dBm), (Warning range: (\[.*?\]dBm)))", data))
                pattern_rx_power_high_threshold = r'Default Rx Power High Threshold\(dBM\)\s*:\s*(-?\d+\.\d+)'
                pattern_rx_power_low_threshold = r'Default Rx Power Low Threshold\(dBM\)\s*:\s*(-?\d+\.\d+)'
                pattern_tx_power_high_threshold = r'Default Tx Power High Threshold\(dBM\)\s*:\s*(-?\d+\.\d+)'
                pattern_tx_power_low_threshold = r'Default Tx Power Low Threshold\(dBM\)\s*:\s*(-?\d+\.\d+)'

                rx_warning_range = ''
                tx_warning_range = ''

                if re.search(pattern_rx_power_high_threshold, data) and re.search(pattern_rx_power_low_threshold, data) and re.search(pattern_tx_power_high_threshold, data) and re.search(pattern_tx_power_low_threshold, data):
                    Rx_Power_High_Threshold = float(re.search(pattern_rx_power_high_threshold, data).group(1))
                    Rx_Power_Low_Threshold = float(re.search(pattern_rx_power_low_threshold, data).group(1))
                    Tx_Power_High_Threshold = float(re.search(pattern_tx_power_high_threshold, data).group(1))
                    Tx_Power_Low_Threshold = float(re.search(pattern_tx_power_low_threshold, data).group(1))

                    rx_warning_range = f'{Rx_Power_Low_Threshold} to {Rx_Power_High_Threshold}'
                    tx_warning_range = f'{Tx_Power_Low_Threshold} to {Tx_Power_High_Threshold}'

                
                if len(rx_tx_warning_range) > 0:
                    rx_warning_range = rx_tx_warning_range[0][1]
                    tx_warning_range = rx_tx_warning_range[0][3]
                elif rx_warning_range and tx_warning_range:
                    if len(re.findall(r"((Rx\d* Power: *-*\d+\.*\d+dBm|--dBm|--|dBm), (Warning range: (\[.*?\]dBm)))", data)) > 0:
                        rx_warning_range = re.findall(r"((Rx\d* Power: *-*\d+\.*\d+dBm|--dBm|--|dBm), (Warning range: (\[.*?\]dBm)))", data)[0][3]
                        tx_warning_range = re.findall(r"((Tx\d* Power: *-*\d+\.*\d+dBm|--dBm|--|dBm), (Warning range: (\[.*?\]dBm)))", data)[0][3]
                    else:
                        rx_warning_range = re.findall(r"((Rx\d* Power: *-*\d+\.*\d+dBm|--dBm|--|dBm), (Working range: (\[.*?\]dBm)))", data)[0][3]
                        tx_warning_range = re.findall(r"((Tx\d* Power: *-*\d+\.*\d+dBm|--dBm|--|dBm), (Working range: (\[.*?\]dBm)))", data)[0][3]


                data_list  = []
                for i, (rx_power, tx_power) in enumerate(rx_tx_powers):
                    data_dict = {
                        "Host_Name": host_name,
                        "interface": interface,
                        "description": description,
                        "port_bw": port_bw,
                        "transceiver_max_bw": transceiver_max_bw,
                        "transceiver_mode": transceiver_mode,
                        "wavelength": wavelength,
                        "transmission_distance": transmission_distance,
                        "internet_address": internet_address,
                        "rx_warning_range": rx_warning_range,
                        "tx_warning_range": tx_warning_range,
                        "transmitter_index": i,
                        "rx_power": re.search(r"( *-*\d+\.*\d+dBm|--dBM|--|dBM)", rx_power).group(1),
                        "tx_power": re.search(r"( *-*\d+\.*\d+dBm|--dBM|--|dBM)", tx_power).group(1)
                    }
                    data_list.append(data_dict)
                
            return df, pd.DataFrame(data_list)

        else:
            kolom1 = ['Host_Name','Interface','Line protocol current state','Link quality grade','Description','IP Sending Frames',
                            'Hardware address','Factor/Module','Standard','FecMode',
                            'The Vendor PN','The Vendor Name','Port BW','Transceiver max BW',
                            'Transceiver Mode','Connector Type','Transmission Distance','WaveLength',
                            'Current system time','Input','Output','Last line protocol up time','Maximal BW',
                            'Current BW','The Maximum Transmit Unit','Internet Address','Vlan ID',
                            'Last 300 seconds input utility rate','Last 300 seconds output utility rate',
                            'Last 10 seconds input utility rate','Last 10 seconds output utility rate']

            kolom2 = ["Host_Name","interface","description","port_bw","transceiver_max_bw",
                            "transceiver_mode","wavelength","transmission_distance","internet_address",
                            "rx_warning_range","tx_warning_range","transmitter_index","rx_power","tx_power"]
            return  pd.DataFrame(columns=kolom1), pd.DataFrame(columns=kolom2)

    except Exception as e:
        print(file_namessss)
        print(m)
        # Handle the exception (e.g., log it, clean up, etc.)
        print(f"An error occurred: {e}")
        # Re-raise the exception to propagate it
        raise