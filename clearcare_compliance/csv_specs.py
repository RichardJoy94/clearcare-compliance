from importlib.resources import files
import yaml

def load_yaml_resource(path: str) -> dict:
    return yaml.safe_load(files("clearcare_compliance.schemas.csv").joinpath(path).read_text(encoding="utf-8"))

PREAMBLE = load_yaml_resource("preamble.yaml")
TALL = load_yaml_resource("tall.yaml")
WIDE = load_yaml_resource("wide.yaml")
