import json
import os

def load_config(config_path):
    config_reject_reason=[]   
    if os.path.exists(config_path):
        try:
            with open(config_path,'r') as config: 
                pipeline_config=json.load(config)
        except json.JSONDecodeError:
            config_reject_reason.append("Invalid Json file")
            return 1, config_reject_reason
        else:
            return 0,pipeline_config
    else:
        config_reject_reason.append("No config file exists")
        return 1, config_reject_reason