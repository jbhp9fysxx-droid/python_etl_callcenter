#import pyodbc
import os
from pathlib import Path
import csv
import sys

# set the source file directory
curr_dir=Path.cwd().resolve().parent
# Validating source file for naming consistency
src_fil_dir= str(curr_dir) + "/src_files/call_center_raw.csv"
def validate_filename(src_file_dir):
    src_reject_reason=[]
    p=Path(src_file_dir)
    filename=p.name

    #validating file name length
    if len(filename)!=19:
        src_reject_reason.append("invalid filename")

    #validating file extesion for .csv
    allowed_file_ext=[".csv"]
    if p.suffix.lower() not in allowed_file_ext:
        src_reject_reason.append(f"invalid file extension: Expected .csv recieved {p.suffix}")
    
    if len(src_reject_reason)!=0:
        return 1, src_reject_reason
    else:
        return 0, src_reject_reason
    
src_reject_flag, src_reject_reason=validate_filename(src_fil_dir)
if src_reject_flag==1:
    print("File naming is invalid and unable to read:")
    for src_rej_reason in src_reject_reason:
        print(src_rej_reason)
    sys.exit()

tgt_fil_dir= str(curr_dir) + "/output/target/call_center_clean.csv"
excp_fil_dir= str(curr_dir) + "/output/exception/call_center_reject.csv"
rows_loaded=0
rows_rejected=0
null_count=0
invalid_time_count=0
invalid_id_count=0
invalid_status_count=0
invalid_row_str=0

def validate_header_columns(f):
    h_reject_reason=[]
    f=[i.lower() for i in f]
    valid_values=["call_id","caller_id","agent_id","call_start_time","call_end_time","call_status"]
    missing_values=[vv for vv in valid_values if vv not in f]
    if len(f)==6:
        h_fields={"call_id":{1:f[0]},
                "caller_id":{2:f[1]},
                "agent_id":{3:f[2]},
                "call_start_time":{4:f[3]},
                "call_end_time":{5:f[4]},
                "call_status":{6:f[5]},
                }
        for key, value in h_fields.items():
            for k,v in value.items():
                if key==v:
                    continue
                elif key!=v and v!="":
                    h_reject_reason.append(f"invalid column name expected {key}, recieved {v}")
                else:
                    h_reject_reason.append(f"expected column {key} is missing at position {k}")
                    break
    else:
        h_reject_reason.append(f"Invalid schema-expected 6 columns but recieved {len(f)}-missing values are {missing_values}")
        return 1,h_reject_reason
    
    if len(h_reject_reason)!=0:
        return 1, h_reject_reason
    else:
        return 0, h_reject_reason

        

def exception_record(fields,r_reason):
    reject_reason=r_reason
    with open(excp_fil_dir,'a',newline='') as efile:
        fields.append(reject_reason)
        writer = csv.writer(efile)
        writer.writerow(fields)
    

def target_records(fields):
    with open(tgt_fil_dir,'a',newline='') as tfile:
        writer = csv.writer(tfile)
        writer.writerow(fields)
        

def mandatory_fields_check(f):
    nvp=[]
    n_reject_reason=[]
    for pos in range(len(f)):
        if f[pos] =="":
            nvp.append(pos)
    for p in nvp:
        if p==0:
            n_reject_reason.append("call_id cant be null")
        if p==3:
            n_reject_reason.append("call_start_time cant be null")
        if p==4:
            n_reject_reason.append("call_end_time cant be null")
        if p==5:
            n_reject_reason.append("call_status cant be null")
    if len(nvp)>0:
        return 1, n_reject_reason
    else:
        return 0, n_reject_reason

def time_check(f):
    t_reject_reason=[]
    for tf in [f[3],f[4]]:
        time_str= str(tf)
        if len(time_str) != 8 or time_str[2] != ":" or time_str[5] != ":":
            t_reject_reason.append("Invalid time format")
        elif time_str[0:2].isdigit()==False or time_str[3:5].isdigit()==False or time_str[6:].isdigit()==False:
            t_reject_reason.append("Invalid time format-time should contain only numeric values")
        else:
            continue
    if len(t_reject_reason)!=0:
        return 1, t_reject_reason
    else:
        return 0, t_reject_reason

def id_is_numeric(f):
    vf={"call_id":str(f[0]),
    "caller_id":str(f[1]),
    "agent_id":str(f[2])}
    i_reject_reason=[]
    for k,v in vf.items():
        if v!="" and v.isdigit()==False:
            i_reject_reason.append(f"{k} must be numeric")
        else:
            continue
    if len(i_reject_reason)!=0:
        return 1, i_reject_reason
    else:
        return 0, i_reject_reason

def status_check(f):
    vf={"call_status":str(f[5]).upper()}
    s_reject_reason=[]
    valid_values=["COMPLETED","DROPPED","FAILED"]
    for k,v in vf.items():
        if v!="" and v not in valid_values:
            s_reject_reason.append(f"not valid value for {k}")
        else:
            continue
    if len(s_reject_reason)!=0:
        return 1, s_reject_reason
    else:
        return 0, s_reject_reason



# Open and vakdate the source file
try:
    with open(src_fil_dir,'r') as file:
        total_records=0
        header_prcsd_flag=False
        for record in file:
            fields=record.strip()
            fields=fields.split(',')
            tgt_header_fields=fields
            excp_header_fields=fields
            if fields==[""]:
                continue
            # Define Target files defnition 
            elif header_prcsd_flag==False:
                # Header fields validation
                invalid_header_flag,h_reject_reason=validate_header_columns(fields)
                if invalid_header_flag==1:
                    break
                excp_header_fields.append("reject_reason")
                header_prcsd_flag=True
                # creating exception table definition
                with open(excp_fil_dir,'w') as efile:
                    efile.truncate()
                    writer = csv.writer(efile)
                    writer.writerow(excp_header_fields)
                # creating Target file definition
                with open(tgt_fil_dir,'w') as tfile:
                    tfile.truncate()
                    writer = csv.writer(tfile)
                    writer.writerow(tgt_header_fields)
                continue
            else:
                total_records+=1
                # validate row structure
                if len(fields)<6:
                    invalid_row_str+=1
                    reject_reason="Invalid row structure- missing required column values"
                    rows_rejected=rows_rejected+1
                    exception_record(fields,reject_reason)
                    continue
                call_id=fields[0] if len(fields)>0 else None
                caller_id=fields[1] if len(fields)>1 else None
                agent_id=fields[2] if len(fields)>2 else None
                call_start_time=fields[3] if len(fields)>3 else None
                call_end_time=fields[4] if len(fields)>4 else None
                call_status=fields[5] if len(fields)>5 else None
                mandatory_fields=[call_id, call_start_time,call_end_time, call_status]
                fields=[call_id, caller_id,agent_id,call_start_time,call_end_time, call_status]
                # Validate records
                nvp,n_reject_reason=mandatory_fields_check(fields)
                invalid_time,t_reject_reason=time_check(fields)
                invalid_id_flag,i_reject_reason=id_is_numeric(fields)
                invalid_status_flag,s_reject_reason=status_check(fields)
                # Aggregate validation results and routing approprate data to target fields
                if nvp==1:
                    null_count+=1
                if invalid_time==1:
                    invalid_time_count+=1
                if invalid_id_flag==1:
                    invalid_id_count+=1
                if invalid_status_flag==1:
                    invalid_status_count+=1
                if nvp==1 or invalid_time==1 or invalid_id_flag==1 or invalid_status_flag==1:
                    r_reason=n_reject_reason+t_reject_reason+i_reject_reason+s_reject_reason
                    rows_rejected=rows_rejected+1
                    exception_record(fields,r_reason)
                    continue
                else:
                    rows_loaded=rows_loaded+1
                    target_records(fields)
                n_reject_reason.clear()
                t_reject_reason.clear()
        
        if total_records==0:
            print("File is empty and nothing to process")
            sys.exit()
        
        if invalid_header_flag==1:
            print("-"*100)
            print("Invalid file header format-File not read")
            print("-"*100)
            for reasons in h_reject_reason:
                print(reasons)
            print("Please re-check the header fields and resend the file")
            print("-"*100)
            sys.exit() 

        print("-"*100)
        print("ETL Summary")
        print("-"*100)
        print(f"Total number of records processed: {total_records}")
        print(f"Total number of rows loaded: {rows_loaded}")
        print(f"Total number of rows rejected: {rows_rejected}")
        print("-"*100)
        print("Rejection breakdown")
        print("-"*100)
        print(f"Invalid column count in a record: {invalid_row_str}")
        print(f"missing mandatory fields count: {null_count}")
        print(f"Invalid time format fields: {invalid_time_count}")
        print(f"Invalid ID fields: {invalid_id_count}") 
        print(f"Invalid call status fields: {invalid_status_count}")
except FileNotFoundError:
    file_name=src_fil_dir[src_fil_dir.rfind("/"):]
    print(f"Error: The file '{file_name}' was not found.")
                


    

