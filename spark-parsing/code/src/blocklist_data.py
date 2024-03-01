from dataclasses import dataclass, field
from datetime import date
from functools import reduce
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
import pyspark.sql.functions as f

from src.utils import to_local_path

@dataclass
class BlocklistData():

    location: Path

    name: str = field(init=False)
    bl_type: str = field(init=False)
    training_df: DataFrame = field(init=False)
    eval_df: DataFrame = field(init=False)

    def __post_init__(self):
        self.name = self.location.name.replace('.', '_')

    def init_spark(self, spark: SparkSession):
        for bl_type in ['ip', 'dn', 'sha1']:
            if bl_type in self.location.name:
                self.bl_type = bl_type
        if self.bl_type is None:
            raise Exception(f'Wrong blocklist input type {self.location}')
        if 'csv' in self.location.name:
            df = spark.read.csv(to_local_path(self.location), escape='"', quote='"', comment='#', header= 'feodo' in self.location.name)
            self.training_df = df.select(f.col(df.columns[1]).alias(self.join_col())).distinct()
        else:
            self.training_df = spark.read.csv(to_local_path(self.location), schema=f'{self.join_col()} STRING', header=False).distinct()

    def join_col(self) -> str:
        if self.bl_type == 'ip':
            return 'ip'
        if self.bl_type == 'dn':
            return 'domain_name'
        if self.bl_type == 'sha1':
            return 'sha1'

    def join_df_name(self) -> str:
        if self.bl_type == 'ip':
            return 'ip'
        if self.bl_type == 'dn':
            return 'domain'
        if self.bl_type == 'sha1':
            return 'cert'

    def experiment_name(self) -> str:
        return f'{self.location.stem}'
