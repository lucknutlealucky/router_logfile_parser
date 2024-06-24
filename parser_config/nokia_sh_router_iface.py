import re
import pandas as pd
import numpy as np

def nk_sh_router_iface(papayukero,ff):
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
                
            if parsing and 'Error: Invalid parameter.' in line:
                is_invalid = True
                continue
                
            if 'show router interface' in line:
                parsing = True

            elif parsing and first_sep == False and '---' in line:
                first_sep = True

            elif is_invalid == False and parsing and first_sep == True and '---' in line:
                first_sep = False
                parsed_paragraph.append(' '.join(parsed_lines))
                parsed_lines = []
                
            elif (parsing and is_invalid) or (parsing and re.search(r'^[A-Z]*:[A-Za-z0-9-]*#', line)): 
                if re.search(r'^[A-Z]*:[A-Za-z0-9-]*#', line) or is_invalid:
                    parsing = False
                    
                if is_invalid == False:
                #     parsed_paragraph.append(' '.join(parsed_lines))
                    parsed_lines = []
                    first_sep = False
                # parsed_lines.append(line)
                hostname = line.split(':')[-1].replace('#', '').replace('\n','')
                
            elif first_sep == True and is_invalid == False and parsing:
                parsed_lines.append(line)

        
        if len(parsed_paragraph) > 0 and all(member.strip() for member in parsed_paragraph): 
            poporo = []
            poporo_index = 0
            qiew = []
            for index, pp in enumerate(parsed_paragraph[0].rstrip('\n').split('\n')):
                if (index%2) == 0 or index == 0:
                    qiew = []
                    mpl = pp.split()
                    if len(mpl) == 5:
                        qiew = [hostname.strip(), mpl[-5],mpl[-4],mpl[-3],mpl[-2],mpl[-1]]
                    else:
                        qiew = [hostname.strip(), ' '.join(mpl[0:-5]),mpl[-4],mpl[-3],mpl[-2],mpl[-1]]
                else:
                    for l in pp.split():
                        qiew.append(l)
                    poporo.append(qiew)
                    
            return pd.DataFrame(poporo, columns = ['hostname', 'Interface-Name', 'Adm', 'Opr(v4/v6)', 'Mode','Port/SapId', 'IP-Address', 'PfxState'])
                       
    except Exception as e:
        print(ff)
        print(poporo)
        print(parsed_paragraph)
        # print(mm)
        # print(i)
        # print(e)
        raise
            