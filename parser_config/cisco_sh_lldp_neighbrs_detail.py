import re
import pandas as pd
import numpy as np

def cisco_sh_lldp_neighbrs_detail(papayukero,ff):
    try:
        parsing = False
        parsed_paragraph = []
        parsed_lines = []
        pewpewpew = []
        hostname = ''
        is_invalid = False
        first_sep = False
        pattern = r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) ([0-2][0-9]|3[01]|[1-9]) ([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])\.\d{3} WIB"
    
        for i in papayukero:
            pewpewpew.append(i)
        
        for line in pewpewpew:
            if re.match(pattern, line):
                continue
                
            if parsing and '% Invalid input detected at' in line:
                is_invalid = True
                continue
                
            if 'show lldp neighbors detail' in line:
                parsing = True

            elif parsing and first_sep == False and '---' in line:
                first_sep = True

            elif is_invalid == False and parsing and first_sep == True and '---' in line:
                first_sep = False
                parsed_paragraph.append(' '.join(parsed_lines))
                parsed_lines = []
                
            elif (parsing and is_invalid) or (parsing and re.search(r'^[A-Za-z0-9/]*:[A-Za-z0-9-]*#', line)): 
                if re.search(r'^[A-Za-z0-9/]*:[A-Za-z0-9-]*#', line) or is_invalid:
                    parsing = False
                    
                if is_invalid == False:
                    parsed_paragraph.append(' '.join(parsed_lines))
                    parsed_lines = []
                    first_sep = False
                # parsed_lines.append(line)
                hostname = line.split(':')[-1].replace('#', '').replace('\n','')
                
            elif first_sep == True and is_invalid == False and parsing:
                parsed_lines.append(line)

        
        if len(parsed_paragraph) > 0 and all(member.strip() for member in parsed_paragraph): 
            pepe = []
            for i in parsed_paragraph:
                patterns = {
                    'Local Interface': r'Local Interface:\s+(.+)',
                    'Parent Interface': r'Parent Interface:\s+(.+)',
                    'Chassis id': r'Chassis id:\s+(.+)',
                    'Port id': r'Port id:\s+(.+)',
                    'Port Description': r'Port Description:\s+(.+)',
                    'System Name': r'System Name:\s+(.+)',
                    'System Description': r'System Description:\s+([\s\S]+?)\n*Time remaining:',
                    'Time remaining': r'Time remaining:\s+(\d+)\s+seconds',
                    'Hold Time': r'Hold Time:\s+(\d+)\s+seconds',
                    'System Capabilities': r'System Capabilities:\s+(.+)',
                    'Enabled Capabilities': r'Enabled Capabilities:\s+(.+)',
                    'IPv4 address': r'IPv4 address:\s+(.+)',
                    'IPv6 address': r'IPv6 address:\s+(.+)',
                    'Peer MAC Address': r'Peer MAC Address:\s+(.+)',
                }
                
                # Extract data using regex
                data = {}
                data['hostname'] = hostname.strip()
                for key, pattern in patterns.items():
                    match = re.search(pattern, i, re.MULTILINE)
                    data[key] = match.group(1).strip() if match else None
                
                # Create a DataFrame
                df = pd.DataFrame([data])
                mid = df['hostname']
                df.drop(labels=['hostname'], axis=1,inplace = True)
                df.insert(0, 'hostname', mid)
                
                pepe.append(df)

            return(pd.concat(pepe))
                
                                    
    except Exception as e:
        print(ff)
        print(i)
        # print(ii)
        # print(mm)
        # print(i)
        # print(e)
        raise
            