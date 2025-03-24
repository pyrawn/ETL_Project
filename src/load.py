import pandas as pd
import yaml
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError  

class Loader:
    
    def __init__(self, config: dict | str):
        self.config = self.load_config(config) if isinstance(config, str) else config
        self.pg_config = self.config["target_postgres"]
        self.engine = self._connect_postgres()

