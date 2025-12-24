import logging

logger= logging.getLogger(__name__)

def validate_header_columns(f,expected_col_count,expected_columns):
    logger.debug("Header validation starting...")
    h_reject_reason=[]
    header=[i.lower() for i in f]
    logger.debug(f"Recieved headers: {header}")
    missing_values=[v for v in expected_columns.keys() if v not in header]
    logger.debug(f"Missing header columns: {missing_values}")

    if len(header)==expected_col_count:
        logger.debug(f"Expected col count of {expected_col_count} successuflly matches the recieved column count of {len(header)}")
        for exp_col, pos in expected_columns.items():
                if exp_col==header[pos]:
                    logger.debug(f"{exp_col} matches {header[pos]}  moving to next column validation...")
                    continue
                elif exp_col!=header[pos] and header[pos]!="":
                    logger.debug(f"{exp_col} do not match recieved { header[pos]} ")
                    h_reject_reason.append(f"invalid column name: expected {exp_col} but recieved {header[pos]}- column added to rejection list")
                else:
                    logger.debug(f"{exp_col} missing at position {pos}: adding to rejection list ")
                    h_reject_reason.append(f"expected column {exp_col} is missing at position {pos}")
                    break
    else:
        logger.error(f"Invalid schema-expected 6 columns but recieved {len(header)}-missing values are {missing_values}")
        h_reject_reason.append(f"Invalid schema-expected 6 columns but recieved {len(header)}-missing values are {missing_values}")
        return 1,h_reject_reason
    if len(h_reject_reason)!=0:
        logger.error("Header processing failed")
        return 1, h_reject_reason
    else:
        logger.info("Header processing sucessfull")
        return 0, h_reject_reason
