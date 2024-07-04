import re
import pandas as pd
import numpy as np

def nk_sh_port_desc(papayukero,ff):
    try:
        parsing = False
        parsed_paragraph = []
        parsed_lines = []
        pewpewpew = []
        hostname = ''
        slot_name = ''
        is_invalid = False
        first_sep = False
        pattern = r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) ([0-2][0-9]|3[01]|[1-9]) ([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])\.\d{3} WIB"
    
        for i in papayukero:
            pewpewpew.append(i)

        line_index = 0
        for index, line in enumerate(pewpewpew):
            if re.match(pattern, line):
                continue

            if 'inspect/10.158.118.241_37378/CommonCollectResult.txt' in ff and parsing:
                print(line.rstrip('\n'))

            if parsing and re.match('^Port Descriptions on (\w*\s*\w*)', line):
                slot_name = line.replace('Port Descriptions on','').strip().rstrip('\n')
                continue
                
            if parsing and ('Error: Invalid parameter.' in line or 'APS Groups' in line or line.startswith('aps-1')):
                is_invalid = True
                
            if 'show port description' in line:
                parsing = True

            elif parsing and first_sep == False and '---' in line:
                first_sep = True

            elif is_invalid == False and parsing and first_sep == True and '===' in line:
                first_sep = False
                parsed_paragraph.append([' '.join(parsed_lines),slot_name])
                parsed_lines = []
                line_index = 0
                
            elif (parsing and is_invalid) or (parsing and re.match(r'^\**[A-Z]*:[A-Za-z0-9-]*\s*[A-Za-z0-9>]*\s*#\s*', line.replace('*','').replace('\n',''))): 
                if re.match(r'^\**[A-Z]*:[A-Za-z0-9-]*\s*[A-Za-z0-9>]*\s*#\s*', line.replace('*','').replace('\n','')) or is_invalid:
                    parsing = False
                    
                    
                if is_invalid == False:
                    # parsed_paragraph.append(' '.join(parsed_lines))
                    parsed_lines = []
                    line_index = 0
                    first_sep = False
                # parsed_lines.append(line)
                hostname = line.split(':')[-1].replace('#', '').replace('\n','')
                
            elif first_sep == True and is_invalid == False and parsing:
                if parsing and re.match(r'[A-Z0-9][A-Z0-9]*[A-Z0-9]*[A-Z0-9]*[A-Z0-9]*\/[A-Z0-9][A-Z0-9]*\/*[A-Z0-9]*[A-Z0-9]*\.*[A-Z0-9]*\.*[A-Z0-9]*\.*[A-Z0-9]*\.*[A-Z0-9]*\.*[A-Z0-9]*[^\S\r\n][^\S\r\n][^\S\r\n]', line):
                    parsed_lines.append(line)
                    line_index = line_index + 1
                else:
                    parsed_lines[line_index - 1] = parsed_lines[line_index - 1].replace('\n','') + line

        
        if len(parsed_paragraph) > 0 and all(member[0].strip() for member in parsed_paragraph): 
            poporo = []
            poporo_index = 0
            qiew = []
            for lolo in parsed_paragraph:
                for index, lrl in enumerate(lolo[0].split('\n')):
                    pp = lrl.strip()
                    if pp:
                        mpl = pp.split()
                        if len(mpl) == 2:
                            poporo.append([hostname.strip(), lolo[1], mpl[0], mpl[1]])
                        else:
                            poporo.append([hostname.strip(), lolo[1], mpl[0],  ' '.join(mpl[1:])])
                
            return pd.DataFrame(poporo, columns = ['hostname', 'Slot Name', 'Port_Id', 'Description'])
                       
    except Exception as e:
        print(ff)
        print(line)
        print(parsed_paragraph)
        # print(mm)
        # print(i)
        # print(e)
        raise
    