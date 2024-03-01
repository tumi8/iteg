#!/usr/bin/env python3

import logging
import os
import sys
from multiprocessing.pool import ThreadPool
from pathlib import Path

import click
import pyspark.sql.functions as f
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DoubleType

from src.experiment import Experiment
from src.tls_graph import TLSGraph
from src.utils import should_save, to_local_path, basic_logging, save_df_csv


@click.command()
@click.option("--cache-dir", type=click.STRING, help="TMP Dir", required=True)
@click.option("--out", type=click.STRING, help="Out Dir", required=True)
@click.option('--parallelization', type=click.INT, help='Optimize Output for this parallelization', default=0)
@click.option('--input-dir', type=click.STRING, help="Input Experiment Dir", required=True)
@click.option('--bidirectional', type=bool, help="Should the graph be treated as bidirectional?", default=False)
@click.option('--epsilon', type=click.FLOAT, help="Evaluation Threshold", default=0.01)
@click.option('--output-type', type=click.Choice(['csv', 'parquet']), default='parquet', help='The output type')
def main(input_dir: Path, cache_dir: Path, out: Path, parallelization: int, epsilon: float, bidirectional: bool, output_type: str):

    input_dir = Path(input_dir)
    tmp_dir = Path(cache_dir)
    out = Path(out)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)
    os.chdir(input_dir)

    basic_logging(out / 'bl_rank.log')

    experiment = Experiment.from_dir(input_dir, bidirectional=bidirectional)
    logging.info(experiment.tls_graph.get_stats())

    spark_config = (
        SparkSession.builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .appName("BL Rank of a TLS Graph")
        .master('local[*]')
        .config('spark.pyspark.python', sys.executable)
    )

    spark = spark_config.getOrCreate()
    sc: SparkContext = spark.sparkContext

    sc.setLogLevel('ERROR')
    sc.setCheckpointDir(to_local_path(tmp_dir / 'checkpoints'))

    if parallelization == 0:
        parallelization = spark.sparkContext.defaultParallelism

    logging.info(f'Starting BL Rank Script. My parallelization: {parallelization}')

    experiment.init_spark(spark)
    tls_graph: TLSGraph = experiment.tls_graph

    partitions = int(os.cpu_count() / 2)

    experiments = []
    for blocklist in experiment.blocklist_data:
        bl_df = blocklist.training_df.join(tls_graph[blocklist.join_df_name()], on=blocklist.join_col()).select('id') \
            .withColumn('score', lit(1.0).astype(DoubleType())) \
            .withColumn('fixed', lit(True))
        experiments.append((blocklist.name, bl_df))

    logging.info(f'Will run for each: {[e[0] for e in experiments]}')


    relation_dst_df = tls_graph.checkpoint_dst_relations(partitions, spark, tmp_dir).cache()
    vertices_df = tls_graph.checkpoint_vertex(partitions, spark, tmp_dir).cache()
    # logging.info('Vertices:\n' + vertices_df.limit(10).toPandas().to_string())

    for sub_experiment in experiments:
        experiment_name, initial_df = sub_experiment
        vertex_initial_name = f'vertices_init_{experiment_name}'

        if should_save(tmp_dir / vertex_initial_name):
            initial_df.join(vertices_df, on='id', how='left_outer').write.bucketBy(partitions, 'id').sortBy('id').option('path', to_local_path(tmp_dir / vertex_initial_name)).saveAsTable(vertex_initial_name, mode='overwrite')

        vertex_init_df = spark.table(vertex_initial_name)
        logging.info(f'The initial Vertex Dataframe (size {vertex_init_df.count()}):')

        current_iteration = vertex_init_df.drop('inDegree').withColumn('old_score', lit(0).astype(DoubleType()))
        last_messages: DataFrame = None

        for iteration in range(100):
            logging.info(f'Starting iteration {iteration} {experiment_name}')
            i_name = f'vertices_iter_{iteration}_{experiment_name}'
            iter_path = tmp_dir / i_name

            changed_threshold = current_iteration.where(f.abs(col('score') - col('old_score')) >= epsilon).count()
            logging.info(f'Changing scores with epsilon {epsilon}: {changed_threshold}')
            if changed_threshold <= 0:
                logging.info(f'No new nodes changed for epsilon {epsilon} in iteration {iteration}')
                break

            messages = current_iteration.select(col('id').alias('dst'), 'score', col('fixed').alias('dstFixed'), col('outDegree').alias('dstOutDegree')).where('score is not null')\
                .join(relation_dst_df, on='dst')

            if last_messages is not None:
                # logging.info('Messages\n' + messages.limit(10).toPandas().to_string())

                last_messages_rev = last_messages.select(col('dst').alias('src'), col('src').alias('dst'), col('score').alias('last_score'))
                messages = messages.join(last_messages_rev, on=['src', 'dst'], how='left_outer')\
                    .select('src', 'dst', 'last_score', 'dstFixed', 'dstOutDegree', 'score')\
                    .withColumn('score', f.when((col('dstFixed').isNull() | ~col('dstFixed')) & col('last_score').isNotNull() & col('dstOutDegree').isNotNull() & (col('dstOutDegree') > 0), col('score') - (col('last_score') / col('dstOutDegree'))).otherwise(col('score')))

                #logging.info('Messages with Rev\n' + messages.limit(10).toPandas().to_string())
                #logging.info('Messages with Rev (not null)\n' + messages.where('last_score is not null').limit(10).toPandas().to_string())
                #messages = messages.select('src', 'dst', col('new_score').alias('score'))

            if iteration % 10 == 0:
                message_name = f'messages_{experiment_name}_{iteration}'
                message_path = tmp_dir / message_name
                if should_save(message_path):
                    messages.repartition(partitions).write.option('path', to_local_path(message_path)).saveAsTable(message_name, mode='overwrite')
                messages.unpersist()
                messages = spark.table(message_name)

            messages.cache()

            messages_grouped = messages.groupBy('src').agg(f.sum('score').alias('score_sum')).withColumnRenamed('src', 'id') \
                .join(vertices_df, on='id', how='left_outer') \
                .withColumn('score', f.when(col('outDegree') > 0, col('score_sum') / col('outDegree'))).drop('score_sum')\
                .select('id', 'score', 'outDegree')

            # logging.info('Messages Grouped\n' + messages_grouped.limit(10).toPandas().to_string())

            next_iteration = current_iteration.drop('old_score') \
                .withColumnRenamed('score', 'old_score').withColumnRenamed('outDegree', 'out2')\
                .join(messages_grouped, on='id', how='outer')\
                .withColumn('score', f.when(col('fixed').isNotNull() & col('fixed'), lit(1.0).astype(DoubleType())).otherwise(col('score')))\
                .select('id', 'fixed', 'score', 'old_score', f.array_max(f.array('outDegree', 'out2')).alias('outDegree'))\
                .fillna(0.0, subset='old_score')

            # logging.info('Next Iteration\n' + next_iteration.limit(10).toPandas().to_string())

            if should_save(iter_path):
                next_iteration.coalesce(partitions).write.bucketBy(partitions, 'id').sortBy('id').option('path', to_local_path(iter_path)).saveAsTable(i_name, mode='overwrite')
            if last_messages is not None:
                last_messages.unpersist()
            current_iteration = spark.table(i_name)
            last_messages = messages

            #logging.info(f"Changing scores with epsilon 0.0001: {current_iteration.where(f.abs(col('score') - col('old_score')) >= .0001).count()}")
            #logging.info(f"Changing scores with epsilon 0.001: {current_iteration.where(f.abs(col('score') - col('old_score')) >= 0.001).count()}")

            current_iteration.where(~col('fixed')).groupBy(f.round('score', 1).alias('score_bucket')).count().orderBy('score_bucket').show()
            current_iteration.where(col('score') > 1.0).show()

            found_nodes = current_iteration.where(col('score') > 0).count()

            logging.info(f'Affected nodes: {found_nodes}')

        logging.info(f'writing final {experiment_name} output')

        with ThreadPool(processes=parallelization) as pool:
            pool.starmap(save_vertex_with_scores(spark, out / experiment_name, current_iteration, vertex_init_df, parallelization, output_type), tls_graph.vertices_dfs)

        current_iteration.unpersist(blocking=True)
    relation_dst_df.unpersist(blocking=True)


def save_vertex_with_scores(spark: SparkSession, output_dir: Path, scores_df: DataFrame, vertex_init_df: DataFrame, parallelization: int, output_type: str):
    def job(name: str, df: DataFrame):
        out_path = output_dir / name
        if should_save(out_path):
            logging.info(f'saving {out_path}')

            cols = [ 'score', 'fixed' ]
            my_scores = scores_df

            if 'degree' in scores_df.columns:
                cols = ['degree'] + cols
            my_scores = my_scores.select('id', *cols)

            if 'evaluation' in vertex_init_df.columns:
                my_scores = my_scores.join(vertex_init_df.select('id', 'evaluation'), on='id', how='left_outer')

            df_r = df.join(my_scores, on='id')
            if df_r.first():
                df_r = df_r.coalesce(parallelization)
                if output_type == 'csv':
                    save_df_csv(df_r, out_path, neo4j=True)
                else:
                    df_r.write.parquet(to_local_path(out_path), mode='overwrite')
            else:
                logging.info(f'Empty Dataframe for {out_path}, skipping..')

    return job


if __name__ == "__main__":
    main()
