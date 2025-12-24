from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def validate_filename(src_file_dir,file_len,allowed_file_ext):
    src_reject_reason=[]
    p=Path(src_file_dir)
    filename=p.name
    logger.debug(f"{filename} validation initiated....")

    #validating file name length
    logger.debug(f"performing filename validation for {filename}....")
    if len(filename)!=file_len:
        logger.error("filename validation failed: invalid filename")
        src_reject_reason.append("invalid filename")

    #validating file extesion for .csv
    if p.suffix.lower() not in allowed_file_ext:
        logger.error(f"invalid file extension: Expected .csv recieved {p.suffix}")
        src_reject_reason.append(f"invalid file extension: Expected .csv recieved {p.suffix}")
    
    if len(src_reject_reason)!=0:
        logger.error("File validations failed")
        return 1, src_reject_reason
    else:
        logger.info(f"{filename} validation successfull")
        return 0, src_reject_reason