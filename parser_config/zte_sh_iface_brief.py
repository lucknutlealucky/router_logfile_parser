import re
import pandas as pd
import numpy as np

def zte_sh_iface_brief(papayukero,ff):
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
            if "Interface" in line:  # Skip header line
                continue
                
            if "Description" in line:
                continue
                
            if re.match(pattern, line):
                continue
                
            if 'show interface brief' in line:
                parsing = True
                # parsed_lines.append(line)
            elif (parsing and re.search(r'^[A-Za-z0-9-]*#', line)): 
                parsing = False
                # parsed_lines.append(line)
                parsed_paragraph.append(parsed_lines)
                parsed_lines = []
                hostname = line.replace('#', '').replace('\n','')
            elif parsing:
                parsed_lines.append(line)

        
        if len(parsed_paragraph) > 0 and len(parsed_paragraph[0]) > 0: 
            result_list = []
            for ppp in parsed_paragraph:
                data = []
                for linex in ppp:
                    line = linex.replace('\n','')
                    parts = line.split()
                    if len(parts) > 0 and len(parts) < 6:
                        if not str(parts[0]).replace(" ", "") == '':
                            interface_data = {
                                "Host NE": hostname,
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
                                "Host NE": hostname,
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
                        elif len(parts) == 7 and not (str(parts[3]).upper() == 'UP' or str(parts[3]).upper() == 'DOWN') and (('OPTICAL' in str(parts[1]).upper()) or ('ELECTRIC' in str(parts[1]).upper()) or ('N/A' in str(parts[1]).upper())):
                            interface_data = {
                                "Host NE": hostname,
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
                                "Host NE": hostname,
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
                                "Host NE": hostname,
                                "Interface": parts[0],
                                "Attribute": parts[1],
                                "Mode": parts[2],
                                "BW": parts[3],
                                "Admin": parts[4],
                                "Phy": parts[5],
                                "Prot": parts[6],
                                "Description": " ".join(parts[7:])
                            }
                            data.append(interface_data)
                        elif not (('INDONESIA' in str(parts[1]).upper()) or ('OPTICAL' in str(parts[1]).upper()) or ('ELECTRIC' in str(parts[1]).upper()) or ('N/A' in str(parts[1]).upper())):
                            interface_data = {
                                "Host NE": hostname,
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

            return df
        else:
            return pd.DataFrame(columns=["Host NE","Interface","Attribute","Mode","BW","Admin","Phy","Prot","Description"])
                                    
    except Exception as e:
        print(ff)
        print(parsed_paragraph)
        # print(ii)
        # print(mm)
        # print(i)
        # print(e)
        raise
            