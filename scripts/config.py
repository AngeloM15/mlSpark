import os
from dataclasses import dataclass

import tomli

PROJECT_PATH = "/".join(os.path.abspath(__file__).split("/")[:-2])
CONFIG_FILE = f"{PROJECT_PATH}/config/config.toml"
DATA_PATH = f"{PROJECT_PATH}/data/"

with open(CONFIG_FILE, mode="rb") as f_data:
    config = tomli.load(f_data)


@dataclass
class Config:
    data_csv: str = DATA_PATH + config["path"]["data_csv"]
    data_parquet: str = DATA_PATH + config["path"]["data_parquet"]
    model: str = DATA_PATH + config["path"]["model"]
    dataset: str = DATA_PATH + config["path"]["dataset"]

    master: str = config["others"]["master"]
    sample: float = config["others"]["sample"]
