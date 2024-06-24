import re
import pandas as pd

def hw_disp_eth_trunk(papayukero,ff):
    try:
        parsing = False
        parsed_paragraph = []
        parsed_lines = []
        hostname = ''
        pewpewpew = []
        for i in papayukero:
            pewpewpew.append(i)
    
        long_line = len(pewpewpew)
        
        for index, line in enumerate(pewpewpew):
            if 'display eth-trunk' in line:
                parsing = True
                parsed_lines.append(line)
            elif (parsing and re.search(r'<(.*?)>', line)): 
                parsing = False
                parsed_lines.append(line)
                parsed_paragraph.append(parsed_lines)
                parsed_lines = []
            elif parsing:
                parsed_lines.append(line)
    
            if (re.search(r'<(.*?)>', line) and long_line > (index + 1)): 
                if '=' in pewpewpew[index + 1]:
                    hostname = line.replace('<', '').replace('>', '').replace('\n','')
            
            if (re.search(r'<(.*?)>', line) and long_line == (index + 1)):
                hostname = line.replace('<', '').replace('>', '').replace('\n','')
    
        
        if len(parsed_paragraph) > 0 and len(parsed_paragraph[0]) > 3:
            parsing = False
            take_flag = False
            parsed_paragraphx = []
            parsed_lines = []
            pedeep = []
    
            de_ep_placeholder = []
    
            for mm in parsed_paragraph:
                for index, pp in enumerate(mm):
                    if "state information is:" in pp:
                        parsing = True
                        parsed_lines.append(pp)
                    elif (((pp == '\n' or pp == ' \n') or (pp == '  \n' or pp == '   \n')) or ((pp == '\n ' or pp == '\n  ') or (pp == '\n   ' or pp == ' \n '))) and (re.search(r'<(.*?)>', pp) or 'state information is:' in mm[index + 1]): 
                        parsing = False
                        parsed_lines.append(pp)
                        if len(parsed_lines) > 5:
                            parsed_paragraphx.append(parsed_lines)
                        parsed_lines = []
                    elif parsing:
                        parsed_lines.append(pp)
    
            if len(parsed_paragraphx) > 7:
                
                for gg in parsed_paragraphx:
                    if len(gg) < 15:
                        continue
                    text = ' '.join(gg) 
                               

                    trunk_iface = re.search(r"(\w+-*\w+)+'s state information is", text).group(1)
                    if re.search(r'LAG ID:\s+(.+?)\s', text):
                        lag_id = re.search(r'LAG ID:\s+(.+?)\s', text).group(1)
                    else:
                        lag_id = ''
                    if re.search(r'Working *Mode:\s+(\w+)', text):
                        working_mode = re.search(r'Working *Mode:\s+(\w+)', text).group(1)#
                    else:
                        working_mode = ''
                    if re.search(r'Preempt Delay\s*\w*:\s+(\w+)', text):
                        preempt_delay = re.search(r'Preempt Delay\s*\w*:\s+(\w+)', text).group(1)
                    else:
                        preempt_delay = ''
                    if re.search(r'Hash A*a*rithmetic:\s+([\w\s]+)', text):
                        hash_arithmetic = re.search(r'Hash A*a*rithmetic:\s+([\w\s]+)', text).group(1).strip()
                    else:
                        hash_arithmetic = ''
                    if re.search(r'System Priority:\s+(\d+)', text):
                        system_priority = re.search(r'System Priority:\s+(\d+)', text).group(1)
                    else:
                        system_priority = ''
                    if  re.search(r'System ID:\s+([\w-]+)', text):
                        system_id = re.search(r'System ID:\s+([\w-]+)', text).group(1)
                    else:
                        system_id = ''
                    if re.search(r'Least Active-linknumber:\s+(\d+)', text):
                        least_active_linknumber = re.search(r'Least Active-linknumber:\s+(\d+)', text).group(1)
                    else:
                        least_active_linknumber = ''
                    if re.search(r'Max Active-linknumber:\s+(\d+)', text):
                        max_active_linknumber = re.search(r'Max Active-linknumber:\s+(\d+)', text).group(1)
                    else:
                        max_active_linknumber = ''
                    if re.search(r'Operate*i*n*g* S*s*tatus:\s+(\w+)', text):
                        operate_status = re.search(r'Operate*i*n*g* S*s*tatus:\s+(\w+)', text).group(1)
                    else:
                        operate_status = ''
                    if re.search(r'Number Of Up Ports* In Trunk:\s+(\d+)', text):
                        num_up_ports = re.search(r'Number Of Up Ports* In Trunk:\s+(\d+)', text).group(1)
                    else:
                        num_up_ports = ''
                    if re.search(r'Timeout Period:\s+(\w+)', text):
                        timeout_period = re.search(r'Timeout Period:\s+(\w+)', text).group(1)
                    else:
                        timeout_period = ''
                    if re.search(r'PortKeyMode:\s+(\w+)', text):
                        port_key_mode = re.search(r'PortKeyMode:\s+(\w+)', text).group(1)
                    else:
                        port_key_mode = ''

                    header_dict = {
                        "Host_Name" : hostname,
                        "Trunk Interface": trunk_iface,
                        "LAG ID": lag_id,
                        "WorkingMode": working_mode,
                        "Preempt Delay": preempt_delay,
                        "Hash arithmetic": hash_arithmetic,
                        "System Priority": system_priority,
                        "System ID": system_id,
                        "Least Active-linknumber": least_active_linknumber,
                        "Max Active-linknumber": max_active_linknumber,
                        "Operate status": operate_status,
                        "Number Of Up Ports In Trunk": num_up_ports,
                        "Timeout Period": timeout_period,
                        "PortKeyMode": port_key_mode
                    }
    
                    local_ports_info = re.findall(r'\w+\d\/\d\/\d\(*\w*\)*\s+\w+\s+\w+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+', text)

                    local_ports = []

                    if local_ports_info:
                        for port in local_ports_info:
                            parts = port.split()
                            local_ports.append({
                                "ActorPortName": parts[0],
                                "Status": parts[1],
                                "PortType": parts[2],
                                "PortPri": int(parts[3]),
                                "PortNo": int(parts[4]),
                                "PortKey": int(parts[5]),
                                "PortState": parts[6],
                                "Weight": int(parts[7])
                            })
                    else:
                        local_ports.append({
                                "ActorPortName": '',
                                "Status": '',
                                "PortType": '',
                                "PortPri": '',
                                "PortNo": '',
                                "PortKey": '',
                                "PortState": '',
                                "Weight": ''
                            })
                    
                    partner_ports_info = re.findall(r'\w+\d\/\d\/\d\(*\w*\)*\s+\d+\s+\w+-\w+-\w+\s+\d+\s+\d+\s+\d+\s+\d+', text)

                    partner_ports = []
                    if partner_ports_info:
                        for port in partner_ports_info:
                            parts = port.split()
                            partner_ports.append({
                                "Partner_ActorPortName": parts[0],
                                "Partner_SysPri": int(parts[1]),
                                "Partner_SystemID": parts[2],
                                "Partner_PortPri": int(parts[3]),
                                "Partner_PortNo": int(parts[4]),
                                "Partner_PortKey": int(parts[5]),
                                "Partner_PortState": parts[6]
                            })

                    else:
                        partner_ports.append({
                                "Partner_ActorPortName": '',
                                "Partner_SysPri": '',
                                "Partner_SystemID": '',
                                "Partner_PortPri": '',
                                "Partner_PortNo": '',
                                "Partner_PortKey": '',
                                "Partner_PortState": ''
                            })

                    
                    combined_ports = []
                    for i in range(len(local_ports)):
                        combined_ports.append({**header_dict, **local_ports[i], **partner_ports[i], "Partner_Weight": 1})
                    
                    df = pd.DataFrame(combined_ports)
                    de_ep_placeholder.append(df)
            else:
                de_ep_placeholder.append(pd.DataFrame(columns = ['Host_Name', 'Trunk Interface', 'LAG ID', 'WorkingMode', 'Preempt Delay',
                                           'Hash arithmetic', 'System Priority', 'System ID',
                                           'Least Active-linknumber', 'Max Active-linknumber', 'Operate status',
                                           'Number Of Up Ports In Trunk', 'Timeout Period', 'PortKeyMode',
                                           'ActorPortName', 'Status', 'PortType', 'PortPri', 'PortNo', 'PortKey',
                                           'PortState', 'Weight', 'Partner_ActorPortName', 'Partner_SysPri',
                                           'Partner_SystemID', 'Partner_PortPri', 'Partner_PortNo',
                                           'Partner_PortKey', 'Partner_PortState', 'Partner_Weight']))
            
            if de_ep_placeholder:
                return pd.concat(de_ep_placeholder,ignore_index=True)
                
            else:
                return pd.DataFrame(columns = ['Host_Name', 'Trunk Interface', 'LAG ID', 'WorkingMode', 'Preempt Delay',
                                            'Hash arithmetic', 'System Priority', 'System ID',
                                            'Least Active-linknumber', 'Max Active-linknumber', 'Operate status',
                                            'Number Of Up Ports In Trunk', 'Timeout Period', 'PortKeyMode',
                                            'ActorPortName', 'Status', 'PortType', 'PortPri', 'PortNo', 'PortKey',
                                            'PortState', 'Weight', 'Partner_ActorPortName', 'Partner_SysPri',
                                            'Partner_SystemID', 'Partner_PortPri', 'Partner_PortNo',
                                            'Partner_PortKey', 'Partner_PortState', 'Partner_Weight'])
                
        else:
            return pd.DataFrame(columns = ['Host_Name', 'Trunk Interface', 'LAG ID', 'WorkingMode', 'Preempt Delay',
                                           'Hash arithmetic', 'System Priority', 'System ID',
                                           'Least Active-linknumber', 'Max Active-linknumber', 'Operate status',
                                           'Number Of Up Ports In Trunk', 'Timeout Period', 'PortKeyMode',
                                           'ActorPortName', 'Status', 'PortType', 'PortPri', 'PortNo', 'PortKey',
                                           'PortState', 'Weight', 'Partner_ActorPortName', 'Partner_SysPri',
                                           'Partner_SystemID', 'Partner_PortPri', 'Partner_PortNo',
                                           'Partner_PortKey', 'Partner_PortState', 'Partner_Weight'])
    except Exception as e:
        print(e)
        print(ff)
        print(text)
        raise