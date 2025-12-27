import logging
from pathlib import Path
import csv


logger=logging.getLogger(__name__)

def create_excp_def(excp_fil_dir,fields):
    with open(excp_fil_dir,'w',newline='') as edef:
        writer = csv.writer(edef)
        writer.writerow(fields)
        logger.debug(f"exception file defnition successfully created")

def create_tgt_def(tgt_fil_dir,fields):
    with open(tgt_fil_dir,'w',newline='') as tdef:
        writer = csv.writer(tdef)
        writer.writerow(fields)
        logger.debug(f"target file defnition successfully created")



def writer_exception(excp_fil_dir,fields,r_reason):
    with open(excp_fil_dir,'a',newline='') as efile:
        excp_fields= fields+ [",".join(r_reason)]
        writer = csv.writer(efile)
        writer.writerow(excp_fields)
        logger.debug(f"record loaded to exception file successfully")
    

def writer_target(tgt_fil_dir,fields):
    with open(tgt_fil_dir,'a',newline='') as tfile:
        writer = csv.writer(tfile)
        writer.writerow(fields)
        logger.info(f"record loaded to Target file successfully")

