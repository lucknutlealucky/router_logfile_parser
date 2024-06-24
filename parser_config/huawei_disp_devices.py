import pandas as pd
import numpy as np
import re

def hw_disp_device(papayukero,ff):
    try:
        parsing = False
        parsed_paragraph = []
        parsed_lines = []
        hostname = ''
        parrent_device = ''
        pewpewpew = []
    
        for i in papayukero:
            pewpewpew.append(i)
    
        long_line = len(pewpewpew)
        
        for index, line in enumerate(pewpewpew):
            if 'display device' in line:
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
            
            filtered_data = []
            
            for papa in parsed_paragraph:
                header_found = False
                first = True
                for line in papa:
                    if first and '---' in line:
                        header_found = True
                        first = False
                        continue
                        
                    elif '---' in line and header_found == True and first == False:
                        header_found = False
                        continue
    
                    elif header_found == False and first == False and '---' in line: 
                        first = True
                    elif header_found == False and first == False:
                        if len(line.split()) > 2:
                            filtered_data.append(line)
                    if (re.search(r"^(.+?)'s Device status:$", line)):
                        parrent_device = re.search(r"^(.+?)'s Device status:$", line).group(1)

            # print(filtered_data)
            # print(parrent_device)
            # print(hostname)
    
            de_ep = []   
            for mm in filtered_data:
                i = mm.rstrip().split()
                to_df = {'Host_Name' : hostname,
                         'Device_name' : parrent_device,
                         'Slot' : i[0],
                         'Type' : i[1],
                         'Online' : i[2],
                         'Register' : i[3], 
                         'Status' : i[4],
                         'Role' : i[5],
                         'LsId' : i[6],
                         'Primary' : i[7]}
                de_ep.append(to_df)
            return pd.DataFrame(de_ep)

        else:
            return pd.DataFrame(columns=['Host_Name', 'Device_name', 'Slot' , 'Type' , 'Online' , 'Register', 'Status', 'Role', 'LsId', 'Primary'])

    except Exception as e:
        print(ff)
        print(parsed_paragraph)
        print(filtered_data)
        print(mm)
        print(i)
        print(e)
        raise
            