from os import walk
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

BASEDIR = Path(__file__).resolve().parent


class ETl:
    
    def __init__(self) -> None:
        self.HOST = "127.0.0.1"
        self.FILE_PATH = "./dump"
        self.DB_NAME= "db.sqlite3"
        self.CONNECTION_STRING = f"sqlite:////{BASEDIR}/{self.DB_NAME}"
        self.engine = self.set_engine()

    def exec(self):
        files = self.get_all_files()
        for file in files:
            self.migrate(file)

    def set_engine(self):
        return create_engine(self.CONNECTION_STRING)

    def get_all_files(self):
        return next(walk(self.FILE_PATH), (None, None, []))[2]

    def set_table_name(self, file_name):
        return file_name.replace(".csv", "")

    def migrate(self, file_name):
        data = pd.read_csv(
            f"{self.FILE_PATH}/{file_name}",
            encoding='windows-1251'
        )
        print(file_name)
        pd.DataFrame(data).to_sql(
            name=self.set_table_name(file_name),
            con=self.engine.connect(),
            if_exists='replace',
            index=False,
            chunksize=10000,
            method='multi'
        )
        print(f"{file_name} migrado")


ETl().exec()
