import pandas as pd
import numpy as np
import re

def hw_ipv6_neighbors(papayukero,ff):

    parsing = False
    parsed_paragraph = []
    parsed_lines = []
    hostname = ''
    

    pewpewpew = []
    for i in papayukero:
        pewpewpew.append(i)

    long_line = len(pewpewpew)
    
    for index, line in enumerate(pewpewpew):
        if 'display ipv6 neighbors brief' in line:
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


    try:
        if len(parsed_paragraph) > 0:
            for qq in parsed_paragraph:

                take = False
                take2 = False
                done = False
                to_use = []
                for ll in qq:
                    if '---' in ll and take2 == False and take == False and done == False:
                        take = True
                    elif take == True and '---' in ll:
                        take = False
                        take2 = True
                    elif take2 == True:
                        if not ('---' in ll or 'Total' in ll): 
                            to_use.append(ll)
                    elif take2 == True and '---' in ll:
                        done = True
                        take2 = False


                lines = ' '.join(to_use).strip().split('\n')[6:-2]
                ipv6_addresses = []
                link_layers = []
                states = []
                types = []
                interfaces = []
                
                # Regex patterns for matching lines
                ipv6_pattern = re.compile(r'^[0-9a-fA-F:]+$')
                link_layer_pattern = re.compile(r'^[0-9a-fA-F-]+$')
                
                # Process each line
                for i in range(0, len(lines)-1, 2):
                    ipv6_address = lines[i].strip()
                    parts = lines[i+1].split()
                    
                    link_layer = parts[0]
                    state = parts[1]
                    type_ = parts[2]
                    interface = parts[3]
                
                    # Append to lists
                    ipv6_addresses.append(ipv6_address)
                    link_layers.append(link_layer)
                    states.append(state)
                    types.append(type_)
                    interfaces.append(interface)
                
                # Create DataFrame
                df = pd.DataFrame({
                    'Host_Name' : hostname,
                    'IPv6_Address': ipv6_addresses,
                    'Link-layer': link_layers,
                    'State': states,
                    'Type': types,
                    'Interface': interfaces
                })
            
                return df
        else:
            return pd.DataFrame(columns = ['Host_Name', 'IPv6_Address', 'Link-layer', 'State', 'Type', 'Interface'])

    except Exception as e:
        print(ff)
        print(parsed_paragraph)
        # Handle the exception (e.g., log it, clean up, etc.)
        print(f"An error occurred on ipv6 neighbor parser: {e}")
        # Re-raise the exception to propagate it
        raise