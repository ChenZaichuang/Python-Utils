import os
import pickle
from datetime import timedelta, datetime


def get_variable_from_local(variable_name_with_path, **kwargs):
    assert "default_value" in kwargs or 'function_value' in kwargs
    if os.path.exists(f'{variable_name_with_path}.pkl'):
        with open(f'{variable_name_with_path}.pkl', 'rb') as f:
            info_map = pickle.load(f)
        if "keep_time" in kwargs or "local_expire_datetime" in kwargs:
            retire_datetime = info_map["created_date"] + timedelta(seconds=int(kwargs["keep_time"])) if "keep_time" in kwargs else datetime.strptime(kwargs["local_expire_datetime"], '%Y-%m-%d %H:%M:%S')
            current_time = datetime.now()
            if current_time <= retire_datetime:
                return info_map["variable_value"]
        else:
            return info_map["variable_value"]
    variable_value = kwargs["default_value"] if "default_value" in kwargs else kwargs["function_value"]()
    with open(f'{variable_name_with_path}.pkl', 'wb') as f:
        pickle.dump({"variable_value": variable_value, "created_date": datetime.now()}, f)
    return variable_value