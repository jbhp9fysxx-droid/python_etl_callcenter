import logging

logger=logging.getLogger(__name__)


def mandatory_fields_check(f):
    logger.debug("starting mandatory field validations...")
    logger.debug(f"revieved fields for validation: {f}")
    nvp=[]
    n_reject_reason=[]
    for pos in range(len(f)):
        if f[pos] =="":
            nvp.append(pos)
    for p in nvp:
        if p==0:
            logger.debug("call_id cant be null")
            n_reject_reason.append("call_id cant be null")
        if p==3:
            logger.debug("call_start_time cant be null")
            n_reject_reason.append("call_start_time cant be null")
        if p==4:
            logger.debug("call_end_time cant be null")
            n_reject_reason.append("call_end_time cant be null")
        if p==5:
            logger.debug("call_status cant be null")
            n_reject_reason.append("call_status cant be null")
    if len(nvp)>0:
        logger.debug(f"Mandatory fields validation failed for {f}")
        return 1, n_reject_reason
    else:
        logger.info(f"Mandatory fields validation successfull for {f}")
        return 0, n_reject_reason


def time_check(f):
    t_reject_reason=[]
    for tf in [f[3],f[4]]:
        time_str= str(tf)
        logger.debug(f"performing basic time field structure and time data validation for {time_str}")
        if len(time_str) != 8 or time_str[2] != ":" or time_str[5] != ":":
            logger.debug(f"invalid time format recieved {time_str}")
            t_reject_reason.append("Invalid time format")
        elif time_str[0:2].isdigit()==False or time_str[3:5].isdigit()==False or time_str[6:].isdigit()==False:
            logger.debug("Invalid time format-time should contain only numeric values")
            t_reject_reason.append("Invalid time format-time should contain only numeric values")
        else:
            continue
    if len(t_reject_reason)!=0:
        logger.debug(f"invalid time fields")
        return 1, t_reject_reason
    else:
        logger.info("Time fields validation success")
        return 0, t_reject_reason


def id_is_numeric(f):
    logger.debug(f"recieved fields for ID validations are: {f}")
    vf={"call_id":str(f[0]),
    "caller_id":str(f[1]),
    "agent_id":str(f[2])}
    i_reject_reason=[]
    for k,v in vf.items():
        if v!="" and v.isdigit()==False:
            logger.debug(f"{k} must be numeric")
            i_reject_reason.append(f"{k} must be numeric")
        else:
            continue
    if len(i_reject_reason)!=0:
        logger.debug("Invalid ID fields")
        return 1, i_reject_reason
    else:
        logger.info("ID fields validation success")
        return 0, i_reject_reason


def status_check(f):

    vf={"call_status":str(f[5]).upper()}
    s_reject_reason=[]
    valid_values=["COMPLETED","DROPPED","FAILED"]
    for k,v in vf.items():
        if v!="" and v not in valid_values:
            logger.debug(f"not valid value for {k}")
            s_reject_reason.append(f"not valid value for {k}")
        else:
            continue
    if len(s_reject_reason)!=0:
        logger.debug("Status validation failed")
        return 1, s_reject_reason
    else:
        logger.info("Status validation success")
        return 0, s_reject_reason


def row_validator(fields):
    nvp,n_reject_reason=mandatory_fields_check(fields)
    invalid_time,t_reject_reason=time_check(fields)
    invalid_id_flag,i_reject_reason=id_is_numeric(fields)
    invalid_status_flag,s_reject_reason=status_check(fields)
   
    if nvp==1 or invalid_time==1 or invalid_id_flag==1 or invalid_status_flag==1:
        r_reason=n_reject_reason+t_reject_reason+i_reject_reason+s_reject_reason
        return nvp,invalid_time,invalid_id_flag,invalid_status_flag, r_reason
    else:
        return nvp,invalid_time,invalid_id_flag,invalid_status_flag, []