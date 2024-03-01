import logging
from dataclasses import dataclass, field
from functools import reduce
from pathlib import Path
from typing import List, Tuple

from graphframes import GraphFrame
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from src.utils import to_local_path, should_save, load_iteg_file


@dataclass
class TLSGraph:

    location: Path
    vertices: List[Tuple[str, Path]]
    relations: List[Tuple[str, Path]]
    bidirectional: bool

    vertices_dfs: List[Tuple[str, DataFrame]] = field(init=False)
    relations_dfs: List[Tuple[str, DataFrame]] = field(init=False)

    @classmethod
    def from_dir(cls, directory: Path, bidirectional: bool = False) -> 'TLSGraph':
        vertices, relations = [], []
        for input_file in directory.iterdir():
            input_name = input_file.name
            for suffix in input_file.suffixes:
                input_name = input_name.replace(suffix, '')
            if '.vertex' in input_file.name:
                vertices.append((input_name, input_file))
            elif '.relation' in input_file.name:
                if not bidirectional or ('deployed_on' not in input_file.name and 'parent_domains' not in input_file.name):
                    relations.append((input_name, input_file))

        return TLSGraph(directory, vertices, relations, bidirectional)

    def get_stats(self) -> str:
        return f'''TLSGraph(vertices=[{ ', '.join(map(lambda x: x[0], self.vertices)) }], relations=[{ ', '.join(map(lambda x: x[0], self.relations)) }], bidirectional={self.bidirectional})'''

    def init_spark(self, spark: SparkSession):
        self.vertices_dfs = [ (n, load_iteg_file(spark, path)) for n, path in self.vertices ]
        self.relations_dfs = [ (n, load_iteg_file(spark, path)) for n, path in self.relations ]

    def vertex_df(self, relations_df: DataFrame = None) -> DataFrame:
        result = []
        for _, df in self.vertices_dfs:
            cols = set(df.columns).intersection(['id', 'inDegree', 'outDegree']) #'domain_name', 'ip', 'sha1',
            result.append(df.select(*cols).fillna(0, subset=['inDegree', 'outDegree']))
        result_df = reduce(lambda x, y: x.unionByName(y, allowMissingColumns=True), result)
        if self.bidirectional:
            g = GraphFrame(result_df.select('id'), relations_df)
            result_df = result_df.drop('inDegree', 'outDegree')\
                .join(g.degrees, on='id', how='left_outer').fillna(0, subset='degree')
        return result_df

    def relation_df(self) -> DataFrame:
        return reduce(lambda x, y: x.union(y), map(lambda x: x[1].select('src', 'dst').withColumn('type', lit(x[0])), self.relations_dfs))

    def checkpoint_vertex(self, parallelization:int, spark, tmp_dir: Path) -> DataFrame:
        name = 'vertices_bi' if self.bidirectional else 'vertices'
        df_path = tmp_dir / name
        if should_save(df_path):
            self.vertex_df().distinct().coalesce(parallelization).write.bucketBy(parallelization, 'id').sortBy('id').option('path', to_local_path(df_path)).saveAsTable(name)
        return spark.table(name)

    def checkpoint_relations(self, parallelization:int, spark, tmp_dir: Path) -> DataFrame:
        name = 'relations_bi' if self.bidirectional else 'relations'
        return self._checkpoint_df(tmp_dir / name, spark, self.relation_df().distinct().repartitionByRange(parallelization, 'src', 'dst'))

    def checkpoint_dst_relations(self, parallelization:int, spark: SparkSession, tmp_dir: Path) -> DataFrame:
        name = 'relations_bi_dst' if self.bidirectional else 'relations_dst'
        df_path = tmp_dir / name
        if should_save(df_path):
            self.relation_df().select('src', 'dst').distinct().coalesce(parallelization).write.bucketBy(parallelization, 'dst').sortBy('dst').option('path', to_local_path(df_path)).saveAsTable(name)
        return spark.table(name)

    def _checkpoint_df(self, tmp_file: Path, spark, df: DataFrame) -> DataFrame:
        if should_save(tmp_file):
            df.write.parquet(to_local_path(tmp_file), mode='overwrite')
        return spark.read.parquet(to_local_path(tmp_file))

    def __getitem__(self, key):
        for n, df in self.vertices_dfs:
            if n == key:
                return df
        for n, df in self.relations_dfs:
            if n == key:
                return df
        raise KeyError(f'Could not find dataframe for {key}')
