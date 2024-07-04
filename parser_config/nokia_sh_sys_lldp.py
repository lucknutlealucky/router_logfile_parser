import re
import pandas as pd
import numpy as np

def nk_sh_sys_lldp(papayukero,ff):
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
                
            if line.strip() == 'show system lldp':
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
            for qq in parsed_paragraph:
                chassis_pattern = re.compile(r"""
                    Chassis\sId\sSubtype\s*:\s*(\d+)\s*
                    Chassis\sId\s*:\s*([\da-f:]+)\s*
                    System\sName\s*:\s*([\w\-]+)\s*
                    System\sDescription\s*:\s*([\s\S]+?)Capabilities\sSupported\s*:\s*([\w\s]+)\s*
                    Capabilities\sEnabled\s*:\s*([\w\s]+)
                    """, re.VERBOSE)
                
                lldp_destination_pattern = re.compile(r"""
                    Index\s(\d+)\s*:\s*([\da-f:]+)
                    """, re.VERBOSE)
                
                lldp_remote_stats_pattern = re.compile(r"""
                    Last\sChange\sTime\s*:\s*([\d\/\s:]+)\s*
                    Rem\sTable\sInserts\s*:\s*(\d+)\s*
                    Rem\sTable\sDeletes\s*:\s*(\d+)\s*
                    Rem\sTable\sDrops\s*:\s*(\d+)\s*
                    Rem\sTable\sAgeouts\s*:\s*(\d+)
                    """, re.VERBOSE)
                
                lldp_sys_mgmt_addr_pattern = re.compile(r"""
                    Address\sSubType\s*:\s*(\d+)\s*\(\w+\)\s*
                    Address\s*:\s*([\d\.]+)\s*
                    Address\sIf\sSubType\s*:\s*(\d+)\s*
                    Address\sIf\sId\s*:\s*(\d+)\s*
                    Address\sOID\s*:\s*([\.\d]+)
                    """, re.VERBOSE)
                
                # Extracting the information
                chassis_info = chassis_pattern.search(qq).groups()
                lldp_destinations = lldp_destination_pattern.findall(qq)
                lldp_remote_stats = lldp_remote_stats_pattern.search(qq).groups()
                # lldp_sys_mgmt_addr = lldp_sys_mgmt_addr_pattern.search(qq).groups()
                
                # Create DataFrames
                chassis_df = pd.DataFrame([chassis_info], columns=[
                    "Chassis Id Subtype", "Chassis Id", "System Name", "System Description",
                    "Capabilities Supported", "Capabilities Enabled"
                ])
                
                lldp_dest_df = pd.DataFrame(lldp_destinations, columns=["Index", "Destination Address"])
                
                lldp_remote_stats_df = pd.DataFrame([lldp_remote_stats], columns=[
                    "Last Change Time", "Rem Table Inserts", "Rem Table Deletes", "Rem Table Drops", "Rem Table Ageouts"
                ])
                # lldp_sys_mgmt_addr_df = pd.DataFrame([lldp_sys_mgmt_addr], columns=[
                #     "Address SubType", "Address", "Address If SubType", "Address If Id", "Address OID"
                # ])
                
                chassis_df = pd.DataFrame([chassis_info], columns=[
                    "Chassis Id Subtype", "Chassis Id", "System Name", "System Description",
                    "Capabilities Supported", "Capabilities Enabled"
                ])

                lldp_dest_df = pd.DataFrame(lldp_destinations, columns=["Index", "Destination Address"])

                lldp_remote_stats_df = pd.DataFrame([lldp_remote_stats], columns=[
                    "Last Change Time", "Rem Table Inserts", "Rem Table Deletes", "Rem Table Drops", "Rem Table Ageouts"
                ])

                # lldp_sys_mgmt_addr_df = pd.DataFrame([lldp_sys_mgmt_addr], columns=[
                #     "Address SubType", "Address", "Address If SubType", "Address If Id", "Address OID"
                # ])

                repeat_length = len(lldp_dest_df)

                # Repeat the chassis, remote stats, and sys mgmt addr DataFrames
                chassis_repeated = pd.concat([chassis_df]*repeat_length, ignore_index=True)
                lldp_remote_stats_repeated = pd.concat([lldp_remote_stats_df]*repeat_length, ignore_index=True)
                # lldp_sys_mgmt_addr_repeated = pd.concat([lldp_sys_mgmt_addr_df]*repeat_length, ignore_index=True)
                
                # Concatenate all the DataFrames horizontally
                final_df = pd.concat([
                    chassis_repeated,
                    lldp_dest_df.reset_index(drop=True),
                    lldp_remote_stats_repeated
                ], axis=1)

                final_df['System Description'] = final_df['System Description'].str.replace('\n','')
                final_df['System Description'] = final_df['System Description'].str.replace('   ','')

                return final_df
                       
    except Exception as e:
        print(ff)
        print(line)
        print(parsed_paragraph)
        # print(mm)
        # print(i)
        # print(e)
        raise
            