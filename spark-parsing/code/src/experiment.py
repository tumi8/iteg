from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict

from pyspark.sql import SparkSession, DataFrame

from src.blocklist_data import BlocklistData
from src.tls_graph import TLSGraph
from src.utils import to_local_path


@dataclass
class Result:
    ip_df: DataFrame
    domain_df: DataFrame
    cert_df: DataFrame
    name_to_df: Dict[str, DataFrame]

@dataclass
class Experiment:
    location: Path
    tls_graph: TLSGraph
    blocklist_data: List[BlocklistData]

    @classmethod
    def from_dir(cls, directory: Path, bidirectional = False) -> 'Experiment':
        tls_graph = TLSGraph.from_dir(directory / 'tls_graph', bidirectional=bidirectional)
        blocklists = []
        for bl_dir in (directory / 'blocklists').iterdir():
            blocklists.append(BlocklistData(bl_dir))
        return Experiment(directory, tls_graph, blocklist_data=sorted(blocklists, key=lambda x: x.name))

    def init_spark(self, spark: SparkSession):
        self.tls_graph.init_spark(spark)
        for blocklist in self.blocklist_data:
            blocklist.init_spark(spark)

    def load_ptp_result(self, spark: SparkSession, name: str) -> Dict[str, Result]:
        result_dir = self.location / 'experiments' / name
        r = dict()
        for exp_dir in result_dir.iterdir():
            if exp_dir.is_dir():
                ip_df = spark.read.parquet(to_local_path(exp_dir / 'ip'))
                dn_df = spark.read.parquet(to_local_path(exp_dir / 'domain'))
                c_df = spark.read.parquet(to_local_path(exp_dir / 'cert'))
                name_to_df = { 'ip': ip_df, 'domain': dn_df, 'cert': c_df }
                r[exp_dir.name] = Result(ip_df=ip_df, domain_df=dn_df, cert_df=c_df, name_to_df=name_to_df)
        return r

