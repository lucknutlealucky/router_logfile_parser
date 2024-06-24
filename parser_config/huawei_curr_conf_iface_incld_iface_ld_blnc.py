import re
import pandas as pd

def huawei_curr_conf_iface_incld_iface_ld_blnc_proc(papayukero,ff):
    try:
        parsing = False
        parsed_paragraph = []
        parsed_lines = []
        hostname = ''
        parrent_device = ''
        pewpewpew = []
        pattern_date = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}'
    
        for i in papayukero:
            pewpewpew.append(i)
    
        long_line = len(pewpewpew)
        
        for index, line in enumerate(pewpewpew):
            if 'display current-configuration interface | include interface|load-balance' in line:
                parsing = True
                # parsed_lines.append(line)
            elif 'Info:' in line:
                continue
            elif re.match(pattern_date, line):
                continue
            elif (parsing and re.search(r'<(.*?)>', line)): 
                parsing = False
                # parsed_lines.append(line)
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
            
            datapp = []

            to_used = []
            starter = 0
            for qq in parsed_paragraph:
                dict_placehold = dict()
                dict_placehold = {'host_name' : hostname}
                for rr in qq:
                    ii = rr.rstrip()
                    if ii.startswith('interface') and not(len(ii.split()) > 2) and not(ii.startswith(' ')):
                        ifacedd = str(ii.split()[1])
                        dict_placehold.update({'interface' : ifacedd})
                        to_used.append(dict_placehold)
                        dict_placehold = dict()
                        dict_placehold = {'host_name' : hostname}
                        starter = starter + 1
                    elif ii.startswith('interface') and len(ii.split()) > 2 and not(ii.startswith(' ')):
                        ifacedd = str(ii.split()[1])
                        suffixed = str(' '.join(ii.split()[2:]))
                        dict_placehold.update({'interface' : ifacedd, 'suffix' : suffixed})
                        to_used.append(dict_placehold)
                        dict_placehold = dict()
                        dict_placehold = {'host_name' : hostname}
                        starter = starter + 1
                    elif ii.startswith(' '):
                        infoedd = str(ii)
                        to_used[starter - 1].update({ii.split()[0] : infoedd})
                    elif len(ii.split()) < 2:
                        datapp.append(ii)

            if len(datapp) > 0:
                print("iface only :",list(set(datapp)))

            if len(to_used) > 0:
                df = pd.DataFrame(to_used)
                return df
                
            else:
                return pd.DataFrame(columns=['interface'])

        else:
            return pd.DataFrame(columns=['interface'])
                        
    except Exception as e:
        print(ff)
        print(parsed_paragraph)
        print(ii)
        # print(mm)
        # print(i)
        # print(e)
        raise
            