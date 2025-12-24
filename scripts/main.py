from logging_factory import get_module_logger
from config_loader import load_config
from file_validations import validate_filename
from reader import fil_reader
from schema_validations import validate_header_columns
from writer import create_excp_def,create_tgt_def,writer_exception,writer_target
from row_validations import row_validator
import os
from pathlib import Path
import csv
import sys
import logging

curr_dir=Path.cwd().resolve().parent
config_fil_dir= str(curr_dir) + "/config/pipeline_config.json"
config_check,config_param=load_config(config_fil_dir)


logger=get_module_logger(__name__,config_param)
logger.debug("Validating source file for naming consistency")
logger.debug("successfull retrieved pipeline configuration file...")

if config_check ==1:
    logger.error("Invalid config file")
    sys.exit()
else:
    logger.info("config file loaded successfully")
    src_fil_dir=config_param["paths"]["source_dir"]+"/"+config_param["paths"]["source_filename"]
    exp_fil_ext=config_param["file_validation"]["allowed_extensions"]
    exp_fil_len=config_param["file_validation"]["expected_filename_length"]
    logger.debug(f"{src_fil_dir} retrieved successfully from pipeline config")
    logger.debug(f"{exp_fil_ext} retrieved successfully from pipeline config")
    logger.debug(f"{exp_fil_len} retrieved successfully from pipeline config")
    logger.debug(f'Initializing file level validation for {config_param["paths"]["source_filename"]}')
    src_reject_flag, src_reject_reason=validate_filename(src_fil_dir,exp_fil_len,exp_fil_ext)
    if src_reject_flag==1:
        logger.error("File naming is invalid and unable to read:")
        for src_rej_reason in src_reject_reason:
            logger.error(f"{src_rej_reason}")
            #print(src_rej_reason)
        logger.error("Pipeline terminated due to invalid source file")
        sys.exit()
    tgt_fil_dir=config_param["paths"]["target_dir"]+"/"+config_param["paths"]["target_filename"]
    excp_fil_dir= config_param["paths"]["exception_dir"]+"/"+config_param["paths"]["exception_filename"]
    rows_loaded=0
    rows_rejected=0
    null_count=0
    invalid_time_count=0
    invalid_id_count=0
    invalid_status_count=0
    invalid_row_str=0

    logger.debug(f"Initializing schema validation for {src_fil_dir}")

    total_records=0
    expected_col_count=config_param["schema"]["expected_column_count"]
    expected_columns=config_param["schema"]["expected_columns"]
    header_prcsd_flag=False
    invalid_header_flag=0
    for record in fil_reader(src_fil_dir):
        fields=record.strip()
        fields=fields.split(',')
        tgt_header_fields=fields.copy()
        excp_header_fields=fields.copy()
        if fields==[""]:
            logger.debug("Blank row arrived skipping the row")
            continue

        elif header_prcsd_flag==False:
            logger.debug("Starting header fields validation")
            invalid_header_flag,h_reject_reason=validate_header_columns(fields,expected_col_count,expected_columns)
            if invalid_header_flag==1:
                logger.error("invalid header columns recieved. pipeline terminated")
                break
            excp_header_fields.append("reject_reason")
            create_excp_def(excp_fil_dir,excp_header_fields)
            create_tgt_def(tgt_fil_dir,fields)
            logger.info("Header validation completed successfully")
            header_prcsd_flag=True
            continue
        else:
            total_records+=1

            if len(fields)< expected_col_count:
                invalid_row_str+=1
                reject_reason=["Invalid row structure- missing required column values"]
                rows_rejected=rows_rejected+1
                writer_exception(excp_fil_dir,fields,reject_reason)
                logger.debug(f"{reject_reason}")
                continue

            call_id=fields[0] if len(fields)>0 else None
            caller_id=fields[1] if len(fields)>1 else None
            agent_id=fields[2] if len(fields)>2 else None
            call_start_time=fields[3] if len(fields)>3 else None
            call_end_time=fields[4] if len(fields)>4 else None
            call_status=fields[5] if len(fields)>5 else None

            fields=[call_id, caller_id,agent_id,call_start_time,call_end_time, call_status]
            
            logger.debug("Initializing row level validation")

            nvp,invalid_time,invalid_id_flag,invalid_status_flag, r_reason=row_validator(fields)
            if nvp==1:
                null_count+=1
            if invalid_time==1:
                invalid_time_count+=1
            if invalid_id_flag==1:
                invalid_id_count+=1
            if invalid_status_flag==1:
                invalid_status_count+=1
            if nvp==1 or invalid_time==1 or invalid_id_flag==1 or invalid_status_flag==1:
                rows_rejected=rows_rejected+1
                writer_exception(excp_fil_dir,fields,r_reason)
                logger.error("row level validation failed, sending record to exception file")
                continue
            else:
                logger.info("Record validation successfull")
                rows_loaded=rows_loaded+1
                writer_target(tgt_fil_dir,fields)
                logger.info("loading record to target table")


    if total_records==0:
            logger.error("File is empty and nothing to process")
            sys.exit()
        
    if invalid_header_flag==1:
        logger.error("-"*100)
        logger.error("Invalid file header format-File not read")
        logger.error("-"*100)
        for reasons in h_reject_reason:
            logger.error(reasons)
        logger.error("Please re-check the header fields and resend the file")
        logger.error("-"*100)
        sys.exit() 

    logger.info("-"*100)
    logger.info("ETL Summary")
    logger.info("-"*100)
    logger.info(f"Total number of records processed: {total_records}")
    logger.info(f"Total number of rows loaded: {rows_loaded}")
    logger.info(f"Total number of rows rejected: {rows_rejected}")
    logger.info("-"*100)
    logger.info("Rejection breakdown")
    logger.info("-"*100)
    logger.info(f"Invalid column count in a record: {invalid_row_str}")
    logger.info(f"missing mandatory fields count: {null_count}")
    logger.info(f"Invalid time format fields: {invalid_time_count}")
    logger.info(f"Invalid ID fields: {invalid_id_count}") 
    logger.info(f"Invalid call status fields: {invalid_status_count}")




    



