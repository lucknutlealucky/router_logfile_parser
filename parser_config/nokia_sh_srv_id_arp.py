import re
import pandas as pd

def nk_sh_srv_id_arp(papayukero,ff):
    def is_mac_like_address(s):
        mac_pattern = re.compile(r'^([0-9a-fA-F]{2}:){4}')
        return bool(mac_pattern.match(s))
    try:
        parsing = False
        parsed_paragraph = []
        parsed_lines = []
        pewpewpew = []

        for i in papayukero:
            pewpewpew.append(i)
        
        for line in pewpewpew:
            if 'show service id' in line:
                parsing = True
                parsed_lines.append(line)
            elif (parsing and (re.search(r'^\**[A-Z]*:[A-Za-z0-9-]*\s*[A-Za-z0-9>]*\s*#\s*', line))):  
                parsing = False
                parsed_lines.append(line)
                parsed_paragraph.append(parsed_lines)
                parsed_lines = []
            elif parsing:
                parsed_lines.append(line)
        
        if len(parsed_paragraph) > 0 and len(parsed_paragraph[0]) > 0: 
            dferr = []
            for papa in parsed_paragraph:
                header_found = False
                filtered_data = []
                first = True
                invalid = False
                service_id = ''
                
                router = ''
                
                for line in papa:
                    if first and '---' in line:
                        header_found = True
                        first = False
                        continue
                    elif '---' in line and first == False:
                        header_found = False
                    elif 'No Matching Entries' in line:
                        invalid = True
                    elif re.search(r'^\**[A-Z]*:[A-Za-z0-9-]*\s*[A-Za-z0-9>]*\s*#\s*', line): 
                        try:
                            # router = match.group().replace('#','').split(':')[1]
                            router = line.replace('#','').split(':')[1].replace('\n','').replace(' ','')
                        except:
                            # router = match.group().replace('#','')
                            router = line.replace('#','').replace('\n','').replace(' ','')
                        header_found = False
                        continue
                        
                    elif header_found == False and 'show service id' in line: 
                        service_id = line.replace('show service id','').replace('\n','').replace('arp','').replace(' ','')
                        continue
                    elif header_found and not('indicates that the corresponding row element may have been truncate' in line or '===' in line):
                        filtered_data.append(line)
                        
                if invalid:
                    continue
                dferr.append(filtered_data)

            # print(dferr)
            rows = []
            for i in dferr:
                for ll in i:
                    parts = ll.rstrip().split()
                    if is_mac_like_address(parts[1]) and len(parts) == 6:
                        rows.append([router, service_id, parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]])
                    elif not(is_mac_like_address(parts[1])) and len(parts) == 5:
                        rows.append([router, service_id, parts[0], '', parts[1], parts[2], parts[3], parts[4]])
                    else:
                        if is_mac_like_address(parts[1]) and len(parts) >= 5:
                            rows.append([router, service_id, parts[0], parts[1], parts[2], parts[3] , ' '.join(parts[4:len(parts)-1]), parts[-1]])
                        elif not(is_mac_like_address(parts[1])) and ('Other' in parts[1] or 'Dynamic' in parts[1]):
                            rows.append([router, service_id, parts[0], '', parts[1], parts[2] , ' '.join(parts[3:len(parts)-1]), parts[-1]])
                        else:
                            print(parts)
                            print(ff)
            

            if len(rows) > 0:
                df = pd.DataFrame(rows, columns=["Host_Name","Service_id","IP_Address", "MAC_Address", "Type", "Expiry", "Interface", "SAP"])
            else:
                columns = ["Host_Name","Service_id","IP_Address", "MAC_Address", "Type", "Expiry", "Interface", "SAP"]
                df = pd.DataFrame(columns=columns)
    
            return df
        else:
            columns = ["Host_Name","Service_id","IP_Address", "MAC_Address", "Type", "Expiry", "Interface", "SAP"]
            df = pd.DataFrame(columns=columns)
            return df
                        
    except Exception as e:
        print(ff)
        # print(parsed_paragraph)
        # print(ii)
        # print(mm)
        # print(i)
        # print(e)
        raise
            