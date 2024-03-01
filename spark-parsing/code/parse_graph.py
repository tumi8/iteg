#!/usr/bin/env python3

import json
import logging
import os
import shutil
import sys
from datetime import date
from functools import reduce
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import List, Tuple

import click
import pyspark.sql.functions as f
import tldextract
import validators as validators
from graphframes import *
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, lit
from pyspark.sql.types import *
from pyspark.sql.types import ArrayType, StringType

from src.dns_scan import DNSScan
from src.schemas import CERTS_PARSED_DDL, HOSTS_DDL, CERTS_RELEVANT_COLUMNS, HOSTS_RELEVANT_COLUMNS
from src.tls_scan import TLSScan
from src.utils import explode_domains, checkpoint_df, to_local_path, should_save, hdfsExist, is_valid_ip, sanitize_url, hdfsDelete, explode_domains_udf, save_df_csv, load_iteg_file

EXEC_LOCATION = Path(__file__).absolute().parent
CERT_PARSE_BIN = EXEC_LOCATION / 'parse-certs' / 'parse-certs'


@click.command()
@click.option("--cache-dir", type=click.Path(dir_okay=True, file_okay=False), help="TMP Dir", required=True)
@click.option("--out", type=click.Path(dir_okay=True, file_okay=False), help="Out Dir", required=True)
@click.option('--overwrite', type=click.BOOL, help='Overwrite existing data', default=False)
@click.option('--parallelization', type=click.IntRange(min=0), help='Optimize Output for this parallelization', default=0)
@click.option('--output-type', type=click.Choice(['csv', 'parquet']), default='parquet', help='The output type')
@click.option('--dns-scan', type=(str, str, str), multiple=True, required=True)
@click.option('--tls-scan', type=(str, str), multiple=True, required=True)
def main(cache_dir: Path, out: Path, overwrite: bool, parallelization: int, output_type: str,
         dns_scan: List[Tuple[str, str, str]], tls_scan: List[Tuple[str, str]]):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s :: %(name)s :: %(levelname)-8s :: %(message)s')

    spark_config = (
        SparkSession.builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .appName("Parse TLS Graph")
        .master('local[*]')
        .config('spark.pyspark.python', sys.executable)
    )

    out = Path(out)
    tmp_dir = Path(cache_dir)

    # One folder for all scans
    hosts_path = tmp_dir / 'hosts'
    http_path = tmp_dir / 'http'
    certs_parsed_path = tmp_dir / 'certs_parsed'
    resolved_dn_ip_path = tmp_dir / 'dn_ip_resolved'

    tls_scans = list(map(lambda x: TLSScan(*x), tls_scan))
    dns_scans = list(map(lambda x: DNSScan(*x), dns_scan))

    logging.info(f'Will process {len(tls_scans)} TLS scans: {list(tls_scans)[:2]}...')
    logging.info(f'Will process {len(dns_scans)} DNS scan files {list(dns_scans)[:2]}...')

    logging.info('Pre Processing files...')

    spark = spark_config.getOrCreate()
    sc: SparkContext = spark.sparkContext
    sc.setLogLevel('ERROR')
    if parallelization == 0:
        parallelization = sc.defaultParallelism

    # Upload src module to workers
    shutil.make_archive('src_archive', 'zip', EXEC_LOCATION, 'src')
    SRC_ARCHIVE = Path('src_archive.zip')
    sc.addPyFile(f'{SRC_ARCHIVE}')

    with ThreadPool() as pool:

        jobs = []

        for i, tls_scan in enumerate(tls_scans):
            jobs.append(pool.apply_async(process_certs, (spark, tls_scan, certs_parsed_path, parallelization, overwrite)))

        for i, tls_scan in enumerate(tls_scans):
            jobs.append(pool.apply_async(process_file(spark, overwrite, parallelization, tls_scan.http(), http_path, tls_scan.date)))
            jobs.append(pool.apply_async(process_file(spark, overwrite, parallelization, tls_scan.hosts(), hosts_path, tls_scan.date, dd=HOSTS_DDL, column_subset=HOSTS_RELEVANT_COLUMNS)))

        for i, dns_scan in enumerate(dns_scans):
            jobs.append(pool.apply_async(pre_process_dns_job(spark, dns_scan, resolved_dn_ip_path, parallelization, overwrite)))

        src_files_changed = max([ j.get() for j in jobs ])

    if src_files_changed:
        logging.info(f'Finished pre processing files to {tmp_dir}')
    else:
        logging.info(f'All Input files are already processed in {tmp_dir}, skipping pre-process. (delete them to re-process)')

    # Node Data
    hosts_df = spark.read.parquet(to_local_path(hosts_path)).drop('source') \
        .withColumn('guid', f.concat(lit("h_"), col('id'), lit('_'), col('scan_date'))) \
        .withColumn('server_name', f.regexp_replace('server_name', r'\.$', ''))

    certs_df = spark.read.parquet(to_local_path(certs_parsed_path)).drop('source') \
        .withColumn("guid", f.concat(lit("c_"), col("sha256"))) \
        .where(f.length('sha256') == 64)  # Sanity check for correctly parsed certs
    http_df = spark.read.parquet(to_local_path(http_path)).drop('source')

    # Edges (with scan time)
    dn_ip_df = spark.read.parquet(to_local_path(resolved_dn_ip_path)).drop('source') \
        .select(f.regexp_replace('domain', r'\.$', '').alias('domain'), 'ip', 'scan_src', 'scan_date')

    cert_san = certs_df.where("altNames is not null") \
        .select('guid', f.explode(f.from_json('altNames', schema=ArrayType(StringType()))).alias('alt_name')) \
        .withColumn('is_ip', is_valid_ip('alt_name')) \
        .select('guid', f.when(~col('is_ip'), col('alt_name')).alias('domain_name'), f.when(col('is_ip'), col('alt_name')).alias('ip'))

    ip_returns_cert = hosts_df.select(f.concat(lit("ip_"), col("ip")).alias('from'),
                                      f.concat(lit("c_"), col("cert_hash")).alias('to'), (col('cert_valid') == 1).alias('valid'), 'scan_date')
    cert_deployed_on_ip = hosts_df.where(col("cert_hash").isNotNull() & (col('error_data').isNull() | ~col('error_data').contains('invalid signature by the server certificate')))\
        .select(f.concat(lit("ip_"), col("ip")).alias('to'),
                f.concat(lit("c_"), col("cert_hash")).alias('from'), (col('cert_valid') == 1).alias('valid'), 'scan_date')

    domain_scanned_ip = hosts_df.select(f.concat(lit("d_"), col('server_name')).alias('from'),
                                        f.concat(lit("ip_"), col("ip")).alias('to'),
                                        (col('cert_valid') == 1).alias('valid'), 'scan_date')

    redirects = http_df.join(hosts_df, on=['scan_date', 'id']) \
        .select(col('server_name').alias('from'), extract_location('Location').alias('to'),
                col('Location').alias('location'), (col('cert_valid') == 1).alias('valid'), 'scan_date') \
        .where(col('from').isNotNull() & col('to').isNotNull()) \
        .select(f.concat(lit('d_'), col('from')).alias('from'), f.concat(lit('d_'), col('to')).alias('to'),
                sanitize_url('location').alias('location'), 'valid', 'scan_date')

    domains_raw = cert_san.select('domain_name') \
        .union(hosts_df.select('server_name')) \
        .union(dn_ip_df.select('domain')) \
        .union(http_df.select(extract_location('Location'))) \
        .where('domain_name is not null')

    logging.info(f'Exploding domains')
    domains_all_exploded = checkpoint_df(spark, 'domains_df', tmp_dir, overwrite, parallelization,
                                         domains_raw.withColumn('domains', explode_domains_udf("domain_name"))
                                         .select(f.explode('domains').alias('domain_name'))
                                         .where('domain_name is not null')
                                         .withColumn('guid', f.concat(f.lit('d_'), col('domain_name'))).distinct())

    logging.info(f'Finished exploding domains. Test: {domains_all_exploded.first()}')

    ips = hosts_df.select('ip') \
        .union(dn_ip_df.select('ip')) \
        .union(cert_san.select('ip')) \
        .where(is_valid_ip('ip')) \
        .where('ip is not null').withColumn('guid', f.concat(lit("ip_"), col("ip")))

    logging.info(f'Generating unique IDs')

    # INFO: id_mapping is used to get globally unique ids over all data, and it maps only scan-wide ids (e.g. cert_ID) to the global id
    df_all_guid_raw = (certs_df.select('guid').distinct()
                       .union(domains_all_exploded.select('guid'))
                       .union(ips.select('guid').distinct())
                       .where(col('guid').isNotNull())
                       )

    window_id = Window.orderBy(col('id_l').asc_nulls_first())
    df_all_guid_raw = df_all_guid_raw \
        .withColumn('id_l', f.monotonically_increasing_id()) \
        .withColumn('id', f.row_number().over(window_id) - lit(1)).drop('id_l')

    id_mapping = checkpoint_df(
        spark,
        'id_mapping',
        tmp_dir,
        overwrite or src_files_changed,
        parallelization,
        df_all_guid_raw,
        buckets='guid'
    )

    logging.info(f'Storing Relations')

    # Save Relations
    save_rel(spark, out, "returns.relation", id_mapping, ip_returns_cert, parallelization, output_type)
    save_rel(spark, out, "deployed_on.relation", id_mapping, cert_deployed_on_ip, parallelization, output_type)

    dn_ip_df_rel = dn_ip_df.withColumn('from', f.concat(lit('d_'), col('domain')))\
        .withColumn('to', f.concat(lit('ip_'), col('ip'))) \
        .select('from', 'to', 'scan_src', 'scan_date') \
        .unionByName(domain_scanned_ip, allowMissingColumns=True)

    save_rel(spark, out, "resolves.relation", id_mapping, dn_ip_df_rel, parallelization, output_type)

    save_rel(spark, out, "contains.relation", id_mapping,
             cert_san.select(col('guid').alias('from'), f.concat(lit('d_'), col('domain_name')).alias('to')).where(col('to').isNotNull())
             .union(
                 cert_san.select(col('guid').alias('from'), f.concat(lit('ip_'), col('ip')).alias('to')).where(col('to').isNotNull())),
             parallelization, output_type)

    save_rel(spark, out, "redirects.relation", id_mapping, redirects, parallelization, output_type)

    save_rel(spark, out, 'parent_domain.relation', id_mapping, domains_all_exploded
             .select(col('guid').alias('from'), gen_subdomain_rel('domain_name').alias('to'))
             .where('to is not null').withColumn('to', f.concat(lit('d_'), col('to'))), parallelization, output_type)

    subdomain_rel = load_iteg_file(spark, out / 'parent_domain.relation') \
        .select(col('dst').alias('src'), col('src').alias('dst'))
    save_rel_simple(out / 'subdomain.relation', subdomain_rel, parallelization, output_type)


    # Compute graph metrics
    logging.info(f'Computing Metrics')
    all_edges = ['returns', 'resolves', 'contains', 'redirects', 'subdomain', 'parent_domain', 'deployed_on']
    edges_df = reduce(lambda x, y: x.union(y),
                      map(lambda x: load_iteg_file(spark, out / f'{x}.relation').select('src', 'dst'), all_edges))

    g = GraphFrame(id_mapping, edges_df)
    in_df = checkpoint_df(spark, 'in_degree', tmp_dir, overwrite, parallelization, g.inDegrees, buckets='id')
    logging.info(f'inDegree done')
    out_df = checkpoint_df(spark, 'out_degree', tmp_dir, overwrite, parallelization, g.outDegrees, buckets='id')
    logging.info(f'outDegree done')

    # Save Vertices
    certs_out = ['subject', 'issuer', 'notBefore', 'notAfter', 'basicConstraints', 'isCa', 'numAltNames']
    certs_out = list(map(lambda x: f.max(x).alias(x), certs_out))  # f.max('valid').alias('valid'), f.min('verifyError').alias('verifyError')

    save_vertex(spark, out, 'cert.vertex', id_mapping, certs_df.groupBy('guid', 'sha1', 'sha256')
                .agg(*certs_out), parallelization, in_df, out_df, out_type=output_type)
    save_vertex(spark, out, 'domain.vertex', id_mapping, domains_all_exploded, parallelization, in_df, out_df, out_type=output_type)
    save_vertex(spark, out, 'ip.vertex', id_mapping, ips, parallelization, in_df, out_df, out_type=output_type)

    spark.stop()
    os.remove(SRC_ARCHIVE)
    logging.info(f'Done')


def process_certs(spark: SparkSession, tls_scan: TLSScan, to_dir: Path, parallelization: int, overwrite: bool) -> bool:
    certs_path = tls_scan.certs()
    to_df_dir = to_dir / f'scan_date={tls_scan.date}' / f'source={tls_scan.name()}'
    if not (should_save(to_df_dir) or overwrite):
        return False

    certs_raw_df = load_csv(spark, certs_path, parallelization)

    parsed_certs_rdd = certs_raw_df.rdd.map(lambda row: f'{row.id},"{row.cert}"') \
        .pipe(f'{CERT_PARSE_BIN} --input - --cert-column 1')

    certs_df = spark.read.csv(parsed_certs_rdd, schema=CERTS_PARSED_DDL, escape='"', quote='"')\
        .select(*CERTS_RELEVANT_COLUMNS)

    save_df(to_df_dir, parallelization, certs_df)
    return True


def pre_process_dns_job(spark, dns_scan: DNSScan, resolved_dn_ip_path: Path, parallelization: int, overwrite: bool):
    def job():
        df_path = resolved_dn_ip_path / f'scan_src={dns_scan.type}' / f'source={dns_scan.name()}'

        if should_save(df_path) or overwrite:
            schema = 'ip string, domain string'

            df = load_csv(spark, dns_scan.location, parallelization, header=False, multiLine=False, schema=schema) \
                .withColumn('scan_date', lit(dns_scan.date))

            save_df(df_path, parallelization, df)
            return True
        return False
    return job


def process_file(spark: SparkSession, overwrite: bool, parallelization: int, read_path: Path, to_path: Path, scan_date: date, dd=None, column_subset: List[str]=None):
    def job():
        source_name = os.path.basename(read_path.parent)
        df_path = to_path / f'scan_date={scan_date}' / f'source={source_name}'
        # try:
        if should_save(df_path) or overwrite:
            df_tmp = load_csv(spark, read_path, parallelization, schema=dd)
            if column_subset is not None:
                df_tmp = df_tmp.select(*column_subset)
            save_df(df_path, parallelization, df_tmp)
            return True
        # except Exception as err:
        #     logging.exception('Could not save csv', exc_info=err)
        return False

    return job


@f.udf(returnType=StringType())
def extract_location(location: str):
    if location is None:
        return None
    extracted = tldextract.extract(location)
    fqdn = extracted.fqdn
    if fqdn != '' and validators.domain(fqdn):
        return fqdn
    return None


def save_vertex(spark: SparkSession, output_dir: Path, name: str, id_mapping: DataFrame, df: DataFrame, parallelization,
                in_df, out_df, out_type='csv'):
    out_path = output_dir / name
    if should_save(out_path, out_type=out_type):
        hdfsDelete(spark, out_path)
        columns = ['id'] + df.columns
        columns.remove('guid')
        df_out = df.distinct().join(id_mapping, on='guid').select(*columns) \
            .join(in_df, on='id', how='left_outer') \
            .join(out_df, on='id', how='left_outer') \
            .fillna(0, subset=['inDegree', 'outDegree'])

        if out_type == 'csv':
            save_df_csv(df_out, out_path, neo4j=True)
        elif out_type == 'parquet':
            df_out.repartitionByRange(parallelization, 'id').write.parquet(to_local_path(out_path))
        else:
            raise Exception(f'Unknown type {out_type}')
        logging.info(f"Wrote {name} as {out_type} to disk!")
    else:
        logging.info(f"Skipping vertex {name}")


def save_rel(spark: SparkSession, output_dir: Path, name: str, id_mapping: DataFrame, df: DataFrame, parallelization,
             out_type='csv'):
    out_path = output_dir / name
    if should_save(out_path, out_type=out_type):
        hdfsDelete(spark, out_path)
        id_src_mapping = id_mapping.withColumnRenamed('guid', 'from').withColumnRenamed('id', 'src')
        id_dst_mapping = id_mapping.withColumnRenamed('guid', 'to').withColumnRenamed('id', 'dst')
        columns = ['src', 'dst'] + df.columns
        columns.remove('from')
        columns.remove('to')

        df_out = df.join(id_src_mapping, on='from').join(id_dst_mapping, on='to')

        if 'scan_date' in columns:
            columns.remove('scan_date')
            agg_columns = [f.sort_array(f.collect_set('scan_date')).alias('scan_dates')]
            if 'sni' in columns:
                columns.remove('sni')
                agg_columns.append(f.sort_array(f.collect_set('sni')).alias('server_name_indicators'))
            if 'valid' in columns:
                columns.remove('valid')
                agg_columns.append(f.max('valid').alias('valid'))
            if 'location' in columns:
                columns.remove('location')
                agg_columns.append(f.sort_array(f.collect_set('location')).alias('locations'))
            if 'scan_src' in columns:
                columns.remove('scan_src')
                agg_columns.append(f.sort_array(f.collect_set('scan_src')).alias('scan_src'))
            df_out = df_out.groupBy(*columns).agg(*agg_columns)
        else:
            df_out = df_out.select(*columns).distinct()

        save_rel_simple(out_path, df_out, parallelization, out_type)
    else:
        logging.info(f"Skipping rel {name}")


def save_rel_simple(output_path: Path, df: DataFrame, parallelization, out_type='csv'):
    if should_save(output_path, out_type = out_type):
        if out_type == 'csv':
            save_df_csv(df, output_path, neo4j=True)
        elif out_type == 'parquet':
            df.repartitionByRange(parallelization, "src", "dst").write.parquet(to_local_path(output_path))
        else:
            raise Exception(f'Unknown type {out_type}')

        logging.info(f"Wrote rel {os.path.basename(output_path)} as {out_type} to disk!")
    else:
        logging.info(f"Skipping rel {os.path.basename(output_path)}")


@f.udf(returnType=ArrayType(StringType()))
def explode_alt_names(altNames):
    if altNames is None:
        return None
    try:
        tmp = json.loads(altNames)
        if type(tmp) == list:
            return tmp
    except json.JSONDecodeError:
        logging.error(f'Could not parse JSON {altNames}')
    return None


@f.udf(returnType=StringType())
def gen_subdomain_rel(domain: str):
    exploded = explode_domains(domain)
    if exploded is not None and len(exploded) > 1:
        return exploded[1]
    return None


def load_csv(spark: SparkSession, file: Path, parallelization: int, schema: str = None, header=True, multiLine=True):
    if not hdfsExist(file):
        raise FileNotFoundError(f'Could not find file {file}')

    load_argument = to_local_path(file)
    if file.suffix == '.xz':
        simple_rdd = spark.sparkContext.parallelize([f'{file}'])
        load_argument = simple_rdd.pipe(f'xzcat --files')

    df = spark.read.csv(load_argument, multiLine=multiLine, header=header, inferSchema=False, enforceSchema=False, escape='"',
                        quote='"',
                        schema=schema, mode='DROPMALFORMED')
    filename = os.path.basename(file)
    if schema is None and 'http' in filename:
        df = df.withColumn('id', col('id').cast(IntegerType()))
    elif schema is None and 'cert_chain' in filename:
        if 'host_id' in df.columns:
            df = df.withColumn('id', col('host_id').cast(IntegerType())).drop('host_id')
        else:
            df = df.withColumn('id', col('id').cast(IntegerType()))
        df = df.withColumn('chain_complete', col('chain_complete').cast(IntegerType()))
    return df.repartition(parallelization)


def save_df(df_path: Path, parallelization: int, df: DataFrame, partitions=None, repatition_range=None):
    df_path = to_local_path(df_path)
    df = df.repartition(parallelization)
    writer = df.write
    if partitions is not None:
        writer = writer.partitionBy(partitions)
    writer.parquet(df_path, mode='overwrite')


def get_valid_reversed_rel(df: DataFrame):
    select_cols = [col('dst').alias('src'), col('src').alias('dst')]
    if 'scan_dates' in df.columns:
        select_cols.append(col('scan_dates'))
    return df.where('valid').select(*select_cols)


if __name__ == "__main__":
    main()
