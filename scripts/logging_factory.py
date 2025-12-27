import logging 

def get_module_logger(module_name,pipeline_config):
    log_dir=pipeline_config["storage"]["local"]["log_dir"]
    default_log_level=pipeline_config["logging"]["default_level"]
    level = getattr(logging, default_log_level.upper(), logging.DEBUG)

    #define logger and set the logger level
    logger =logging.getLogger(module_name)
    logger.setLevel(level)

    logger.propagate=False

    if not logger.handlers:
        # Setup handlers
        console=logging.StreamHandler()
        console.setLevel(logging.ERROR)

        file= logging.FileHandler(log_dir+"/"+module_name+".log")
        file.setLevel(logging.INFO)

        debug=logging.FileHandler(log_dir+"/"+module_name+"_debug.log")
        debug.setLevel(logging.DEBUG)

        #setup formatter
        formatter= logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")


        console.setFormatter(formatter)
        file.setFormatter(formatter)
        debug.setFormatter(formatter)

        # attach handlers to logger 

        logger.addHandler(console)
        logger.addHandler(file)
        logger.addHandler(debug)

    return logger

