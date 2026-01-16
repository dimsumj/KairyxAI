# json_encoder.py

import json
import numpy as np

class NpEncoder(json.JSONEncoder):
    """
    Custom JSON encoder to handle NumPy data types.
    Converts NumPy integers and floats to their Python equivalents.
    """
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)