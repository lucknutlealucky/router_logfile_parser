import re
import pandas as pd
import numpy as np

def csc_sh_interfaces(papayukero,ff):
    try:
        parsing = False
        parsed_paragraph = []
        parsed_lines = []
        pewpewpew = []
        hostname = ''
        is_invalid = False
        pattern = r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) ([0-2][0-9]|3[01]|[1-9]) ([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])\.\d{3} WIB"
    
        for i in papayukero:
            pewpewpew.append(i)
        
        for line in pewpewpew:
            if re.match(pattern, line):
                continue
                
            if parsing and '% Invalid input detected at' in line:
                is_invalid = True
                continue
                
            if line.replace('\n','').replace(' ','') == 'showinterfaces':
                parsing = True
                
            elif (parsing and is_invalid) or (parsing and (re.search(r'^(\S+) is up', line) or re.search(r'^[A-Za-z0-9/]*:[A-Za-z0-9-]*#', line))): 
                if re.search(r'^[A-Za-z0-9/]*:[A-Za-z0-9-]*#', line) or is_invalid:
                    parsing = False

                if is_invalid == False:
                    parsed_paragraph.append(' '.join(parsed_lines))
                    parsed_lines = []
                    parsed_lines.append(line)

                hostname = line.split(':')[-1].replace('#', '').replace('\n','')
                
            elif is_invalid == False and parsing:
                parsed_lines.append(line)

        
        if len(parsed_paragraph) > 0 and all(member.strip() for member in parsed_paragraph): 
            # print(parsed_paragraph)
            qqq = []
            for qq in parsed_paragraph:
                if len(qq) > 10:
                    regex_patterns = {
                    'Interface': r'^(\S+) is up',
                    'Status': r'^\S+ is (\w+),',
                    'Interface state transitions': r'Interface state transitions: (\d+)',
                    'Dampening enabled': r'Dampening enabled: penalty (\d+), not suppressed',
                    'Hardware': r'Hardware is (\S+), address is (\S+) \(bia (\S+)\)',
                    'Layer 1 Transport Mode': r'Layer 1 Transport Mode is (\S+)',
                    'Description': r'Description: (.+)',
                    'Internet address': r'Internet address is (\S+)',
                    'MTU': r'MTU (\d+) bytes, BW (\d+) Kbit \(Max: (\d+) Kbit\)',
                    'reliability': r'reliability (\d+)/(\d+), txload (\d+)/(\d+), rxload (\d+)/(\d+)',
                    'Encapsulation': r'Encapsulation (\S+)',
                    'Full-duplex': r'Full-duplex, (\d+Mb/s), (\S+), link type is (\S+)',
                    'output flow control': r'output flow control is (\S+), input flow control is (\S+)',
                    'Carrier delay': r'Carrier delay \(up\) is (\d+) msec, Carrier delay \(down\) is (\d+) msec',
                    'Last link flapped': r'Last link flapped (\S+)',
                    'Last input': r'Last input (\S+), output (\S+)',
                    'Last clearing': r'Last clearing of "show interface" counters (\S+)',
                    '30 second input rate': r'30 second input rate (\d+) bits/sec, (\d+) packets/sec',
                    '30 second output rate': r'30 second output rate (\d+) bits/sec, (\d+) packets/sec',
                    'packets input': r'(\d+) packets input, (\d+) bytes, (\d+) total input drops',
                    'Received broadcast packets': r'Received (\d+) broadcast packets, (\d+) multicast packets',
                    'input errors': r'(\d+) input errors, (\d+) CRC, (\d+) frame, (\d+) overrun, (\d+) ignored, (\d+) abort',
                    'packets output': r'(\d+) packets output, (\d+) bytes, (\d+) total output drops',
                    'Output broadcast packets': r'Output (\d+) broadcast packets, (\d+) multicast packets',
                    'output errors': r'(\d+) output errors, (\d+) underruns, (\d+) applique, (\d+) resets',
                    'output buffer failures': r'(\d+) output buffer failures, (\d+) output buffers swapped out',
                    'carrier transitions': r'(\d+) carrier transitions'
                    }

                    data = {key: None for key in regex_patterns}
                    data['hostname'] = hostname.strip()
                    
                    for key, pattern in regex_patterns.items():
                        match = re.search(pattern, qq, re.MULTILINE)
                        if match:
                            data[key] = match.group(1).strip().replace('  ','').replace('\n','')
                            
                    df = pd.DataFrame([data])
                    df = df.map(lambda x: x if not isinstance(x, tuple) else ', '.join(x))
                    mid = df['hostname']
                    df.drop(labels=['hostname'], axis=1,inplace = True)
                    df.insert(0, 'hostname', mid)
                    
                    qqq.append(df)

            if qqq:
                return pd.concat(qqq)
                
                                    
    except Exception as e:
        print(ff)
        print(qq)
        # print(ii)
        # print(mm)
        # print(i)
        # print(e)
        raise
            