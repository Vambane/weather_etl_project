import yaml
import os

def load_config(path: str = "config.yaml") -> dict:
    full_path = os.path.abspath(path)
    if not os.path.isabs(path):
        full_path = os.path.join(os.getcwd(), path)

    with open(full_path, "r") as f:
        config = yaml.safe_load(f) or {}

    for key in ("paths", "settings"):
        if key not in config:
            config[key] = {}

    config_dir = os.path.dirname(full_path)
    for key, value in config["paths"].items():
        if isinstance(value, str) and key.endswith("_path") and not os.path.isabs(value):
            config["paths"][key] = os.path.join(config_dir, value)

    return config