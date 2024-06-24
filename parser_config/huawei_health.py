import pandas as pd
import numpy as np
import re

def hw_disp_health(papayukero,ff):

    for p in ['10.0.135.101', '10.0.135.113', '10.0.135.126','10.1.133.33','10.1.148','10.1.160','10.1.168','10.1.176','10.10.36.46','10.10.40.46']:
        if p in ff:
            return pd.DataFrame(columns=['Host_Name','Slot','Device','CPU_Usage','Memory_Usage','Memory_(Used/Total)'])
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
            if 'display health' in line:
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
    
            de_ep = []   
            for mm in filtered_data:
                i = mm.rstrip().split()
                to_df = {'Host_Name' : hostname,
                        'Slot' : i[0],
                        'Device' : i[1],
                        'CPU_Usage' : i[2],
                        'Memory_Usage' : i[3], 
                        'Memory_(Used/Total)' : i[4]}
                de_ep.append(to_df)
            return pd.DataFrame(de_ep)

        else:
            return pd.DataFrame(columns=['Host_Name','Slot','Device','CPU_Usage','Memory_Usage','Memory_(Used/Total)'])

    except Exception as e:
        print(ff)
        # print(parsed_paragraph)
        # print(filtered_data)
        # print(mm)
        # print(i)
        print(e)
        # raise
        return pd.DataFrame(columns=['Host_Name','Slot','Device','CPU_Usage','Memory_Usage','Memory_(Used/Total)'])
            