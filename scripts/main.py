from logging_factory import get_module_logger
from config_loader import load_config
from file_validations import validate_filename
from reader import fil_reader,s3_reader
from schema_validations import validate_header_columns
from writer import create_excp_def,create_tgt_def,writer_exception,writer_target
from row_validations import row_validator
from s3_utils import upload_to_s3
from pathlib import Path
import sys
import logging

def main():
    curr_dir=Path.cwd().resolve().parent
    config_fil_dir= str(curr_dir) + "/config/pipeline_config.json"
    config_check,config_param=load_config(config_fil_dir)


    logger=get_module_logger(__name__,config_param)

    logger.debug("checking for ETL pipeline configuration file and its contents...")

    if config_check ==1:
        logger.error("Invalid config file: Pipeline terminated")
        sys.exit()
    else:
        logger.info("config file is valid and parameters loaded successfully")

        storage_type = config_param["storage"]["type"]
        exp_fil_ext=config_param["file_validation"]["allowed_extensions"]
        exp_fil_len=config_param["file_validation"]["expected_filename_length"]
        excp_fil_dir= config_param["storage"]["local"]["exception_dir"]+"/"+config_param["storage"]["local"]["exception_filename"]
        tgt_fil_dir=config_param["storage"]["local"]["target_dir"]+"/"+config_param["storage"]["local"]["target_filename"]

        # loading param based on storage type- s3 vs local 
        if storage_type=="local":
            logger.info(f"-"*50)
            src_fil_dir=config_param["storage"]["local"]["source_dir"]+"/"+config_param["storage"]["local"]["source_filename"]
            tgt_fil_dir=config_param["storage"]["local"]["target_dir"]+"/"+config_param["storage"]["local"]["target_filename"]
            excp_fil_dir= config_param["storage"]["local"]["exception_dir"]+"/"+config_param["storage"]["local"]["exception_filename"]
            logger.info(f"Source filename= {src_fil_dir}")
            logger.info(f"expected file length={exp_fil_len}")
            logger.info(f"expected file extension={exp_fil_ext}")
            logger.info(f"Target filename= {tgt_fil_dir}")
            logger.info(f"Exception filename={excp_fil_dir}")
            logger.info(f"-"*50)
            logger.info(f'Initializing source file validations')
            logger.info(f"-"*50)
            filename_to_validate = Path(src_fil_dir).name 
        elif storage_type=="s3":
            logger.info(f"-"*50)
            s3_bucket=config_param["storage"]["s3"]["bucket"]
            src_file_key=config_param["storage"]["s3"]["source_key"]
            tgt_file_key=config_param["storage"]["s3"]["target_key"]
            excp_file_key= config_param["storage"]["s3"]["exception_key"]
            logger.info(f"Source filename= {src_file_key}")
            logger.info(f"expected file length={exp_fil_len}")
            logger.info(f"expected file extension={exp_fil_ext}")
            logger.info(f"Target filename= {tgt_file_key}")
            logger.info(f"Exception filename={excp_file_key}")
            logger.info(f"-"*50)
            logger.info(f'Initializing source file format validations')
            logger.info(f"-"*50)
            filename_to_validate = Path(src_file_key).name
        else:
            logger.error(f"Unsupported storage type: {storage_type}")
            sys.exit() 
        
        # call file naming format validator function
        src_reject_flag, src_reject_reason=validate_filename(filename_to_validate,exp_fil_len,exp_fil_ext)
        

        if src_reject_flag==1:
            logger.error("File is invalid and unable to pricess for the below reasons:")
            for src_rej_reason in src_reject_reason:
                logger.error(f"{src_rej_reason}")
            logger.error("Pipeline terminated due to invalid source file naming format")
            sys.exit()
        
        rows_loaded=0
        rows_rejected=0
        null_count=0
        invalid_time_count=0
        invalid_id_count=0
        invalid_status_count=0
        invalid_row_str=0

        logger.debug(f"Initializing schema validation")

        total_records=0
        expected_col_count=config_param["schema"]["expected_column_count"]
        expected_columns=config_param["schema"]["expected_columns"]
        header_prcsd_flag=False
        invalid_header_flag=0


        #Choosing storage type s3 or local for processing source file 
        if storage_type == "local":
            record_stream = fil_reader(src_fil_dir)
        elif storage_type == "s3":
            record_stream = s3_reader(s3_bucket, src_file_key)
            
        for record in record_stream:
            fields=record.strip()
            fields=fields.split(',')
            tgt_header_fields=fields.copy()
            excp_header_fields=fields.copy()

            if fields==[""]:
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
                logger.info(f"-"*50)
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
                
                logger.debug(f"Validating row number {total_records}")

                nvp,invalid_time,invalid_id_flag,invalid_status_flag, r_reason=row_validator(fields)
                
                # updating metrics counters based on validation results 
                if nvp==1:
                    null_count+=1
                if invalid_time==1:
                    invalid_time_count+=1
                if invalid_id_flag==1:
                    invalid_id_count+=1
                if invalid_status_flag==1:
                    invalid_status_count+=1

                # Routing records that failed validations to execption file
                if nvp==1 or invalid_time==1 or invalid_id_flag==1 or invalid_status_flag==1:
                    rows_rejected=rows_rejected+1
                    writer_exception(excp_fil_dir,fields,r_reason)
                    logger.debug(f"row {total_records} validation failed, sending record to exception file")
                    continue
                else:
                    #Loading records to Target if record is valid
                    logger.info(f"row {total_records} validation successfull")
                    rows_loaded=rows_loaded+1
                    writer_target(tgt_fil_dir,fields)
                    logger.info("loaded record to target table")

        #Stopping the pipeline if empty file
        if total_records==0:
                logger.error("File is empty and nothing to process-peipeline terminated")
                sys.exit()
        #stopping the pipeline if invalid header column
        if invalid_header_flag==1:
            logger.error("-"*100)
            logger.error("Invalid file header format-File not read")
            logger.error("-"*100)
            for reasons in h_reject_reason:
                logger.error(reasons)
            logger.error("Please re-check the header fields and resend the file")
            logger.error("-"*100)
            sys.exit() 

        # uploading target and exception files to s3
        if storage_type=="s3":
            upload_to_s3(config_param)

        # Calcuating ETL metrics 
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

if __name__== "__main__":
    main()
