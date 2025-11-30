import yaml
import os

def load_config(path: str ="config.yaml") -> dict:
    full_path = os.path.join(os.getcwd(), path)

    with open(full_path, "r") as f:
        return yaml.safe_load(f)
    