import re
import pandas as pd
import numpy as np

def nk_sh_sys_lldp_negbr(papayukero,ff):
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

            # if 'inspect/114.0.146.144_17206/CommonCollectResult.txt' in ff and parsing:
            #     print(line)
                
            if parsing and ('Error: Invalid parameter.' in line):
                is_invalid = True
                
            if 'show system lldp neighbor' in line:
                parsing = True

            elif parsing and first_sep == False and '---' in line:
                first_sep = True

            elif is_invalid == False and parsing and first_sep == True and '===' in line:
                first_sep = False
                parsed_paragraph.append(' '.join(parsed_lines))
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
                parsed_lines.append(line)

        
        if len(parsed_paragraph) > 0 and all(member.strip() for member in parsed_paragraph): 
            poporo = []
            poporo_index = 0
            qiew = []
            for lolo in parsed_paragraph:
                for index, lrl in enumerate(lolo.split('\n')):
                    pp = lrl.strip()
                    if pp:
                        mpl = pp.split()
                        if len(mpl) == 7:
                            poporo.append([hostname.strip(), mpl[0], mpl[1], mpl[2], mpl[3], ' '.join(mpl[4:5]), mpl[6]])
                        elif len(mpl) == 6:
                            poporo.append([hostname.strip(), mpl[0], mpl[1], mpl[2], mpl[3], mpl[4], mpl[5]])
                        
            return pd.DataFrame(poporo, columns = ['hostname', 'Lcl Port', 'Scope', 'Remote Chassis ID', 'Index', 'Remote Port', 'Remote System Name'])
                       
    except Exception as e:
        print(ff)
        print(line)
        print(parsed_paragraph)
        # print(mm)
        # print(i)
        # print(e)
        raise
            