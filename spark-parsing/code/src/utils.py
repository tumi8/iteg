import csv
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Union, Iterable
from urllib.parse import quote

import validators
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import BooleanType, StringType, ArrayType
from tldextract import tldextract
import pyspark.sql.functions as f


def basic_logging(log_file: Path):
    handlers = None
    if log_file is not None:
        handlers = [logging.FileHandler(log_file, 'a', errors='backslashreplace'), logging.StreamHandler()]
    logging.basicConfig(level=logging.INFO, format='%(asctime)s :: %(name)s :: %(levelname)-8s :: %(message)s', handlers=handlers)

def update_spark_log_level(spark, log_level="info"):
    spark.sparkContext.setLogLevel(log_level)
    log4j = spark._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger("my custom Log Level")
    return logger


def get_file_suffixes(path: Path, suffixes=['xz', 'zst']):
    if path.exists():
        return path
    for suffix in suffixes:
        new_path = Path(f'{path}.{suffix}')
        if new_path.exists():
            return new_path
    raise FileNotFoundError(f'Could not find file {path} with potential suffixes {suffixes}')


@f.udf(returnType=ArrayType(StringType()))
def explode_domains_udf(domain: str):
    return explode_domains(domain)


def explode_domains(domain: str):
    if domain is None:
        return None
    if validators.domain(domain) or validators.domain(domain.replace('*.', '')):
        extracted = tldextract.extract(domain, include_psl_private_domains=True)
        d_list = extracted.subdomain.split(".")
        d_list.append(extracted.domain)
        d_list.append(extracted.suffix)
        d_t_list = [e for e in d_list if e]
        r_list = [".".join(d_t_list[i:]) for i in range(len(d_t_list) - 1)]
        return r_list
    return None


def checkpoint_df(spark: SparkSession, name: str, storage_dir: Path, overwrite: bool, parallelization: int,
                  df: DataFrame, partitions=None, buckets=None) -> DataFrame:
    df_file = storage_dir / name

    if not overwrite and not should_save(df_file):
        logging.debug(f'Read from last run: {df_file}')
        return spark.read.parquet(to_local_path(df_file))
    if buckets is None:
        df = df.repartition(parallelization)
    else:
        df = df.repartitionByRange(parallelization, buckets)
    writer = df.write.format('parquet').option('path', to_local_path(df_file)).mode('overwrite')
    if partitions is not None:
        writer = writer.partitionBy(partitions)
    writer.save()
    return spark.read.parquet(to_local_path(df_file))


def should_save(out_path: Path, out_type: str = None) -> bool:
    if out_type == 'csv':
        if Path(f'{out_path}.csv').exists():
            return False
    out_success = out_path / '_SUCCESS'
    return not out_success.exists()


def hdfsGetFS(spark, path: str):
    sc = spark.sparkContext
    if path.startswith("file:"):
        return sc._jvm.org.apache.hadoop.fs.FileSystem.getLocal(sc._jsc.hadoopConfiguration())
    else:
        return sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())


def hdfsExist(path: str):
    return Path(path).exists()


def hdfsDelete(spark: SparkSession, path: Path):
    shutil.rmtree(path, ignore_errors=True)


def to_local_path(f: Union[str, Path]) -> str:
    return f'file://{os.path.abspath(f)}'


# Copy from validators.ipv4
def ipv4(value):
    groups = value.split('.')
    if len(groups) != 4 or any(not x.isdigit() for x in groups):
        return False
    return all(0 <= int(part) < 256 for part in groups)


# Copy from validators.ipv6
def ipv6(value):
    ipv6_groups = value.split(':')
    if len(ipv6_groups) == 1:
        return False
    ipv4_groups = ipv6_groups[-1].split('.')

    if len(ipv4_groups) > 1:
        if not ipv4(ipv6_groups[-1]):
            return False
        ipv6_groups = ipv6_groups[:-1]
    else:
        ipv4_groups = []

    max_groups = 6 if ipv4_groups else 8
    if len(ipv6_groups) > max_groups:
        return False

    count_blank = 0
    for part in ipv6_groups:
        if not part:
            count_blank += 1
            continue
        try:
            num = int(part, 16)
        except ValueError:
            return False
        else:
            if not 0 <= num <= 65536:
                return False

    if count_blank < 2:
        return True
    elif count_blank == 2 and not ipv6_groups[0] and not ipv6_groups[1]:
        return True
    return False


@f.udf(returnType=BooleanType())
def is_valid_ip(ip: str):
    return ip is not None and (ipv4(ip) or ipv6(ip))


@f.udf(returnType=StringType())
def sanitize_url(url: str):
    if url is None:
        return ''
    return quote(url, safe=':/?=&')

def run_cmd(command: str) -> str:
    logging.info(f'Running command: {command}')
    return subprocess.check_output(command, shell=True, text=True)

def save_df_csv(df: DataFrame, to_file: Path, neo4j: bool = False):
    if not 'csv' in to_file.suffix:
        to_file = Path(f'{to_file}.csv')

    if neo4j:
        csv_header = list(to_neo4j_header(df.columns))

        for spark_header, csv_h in zip(df.columns, csv_header):
            if '[]' in csv_h:
                df = df.withColumn(spark_header, f.concat_ws(';', f.col(spark_header)))
    else:
        csv_header = df.columns

    df.write.csv(f'file://{to_file}.tmp', header=False)

    with to_file.open(mode='w') as output_file:
        csv.writer(output_file).writerow(csv_header)

    subprocess.check_output(f'cat {to_file}.tmp/part* >> {to_file}', shell=True, text=True)
    shutil.rmtree(f'{to_file}.tmp')

def load_iteg_file(spark: SparkSession, file_path: Path):
    if not file_path.exists():
        file_path = get_file_suffixes(file_path, ['csv', 'csv.zst'])
    if '.csv' in file_path.name:
        df = spark.read.csv(to_local_path(file_path), header=True)
        for old_header, new_header in zip(df.columns, from_neo4j_header(df.columns)):
            df = df.withColumnRenamed(old_header, new_header)
        return df
    else:
        return spark.read.parquet(to_local_path(file_path))


def to_neo4j_header(header):
    for h in header:
        if h == 'id':
            yield ':ID'
        elif h == 'src':
            yield ':START_ID'
        elif h == 'dst':
            yield ':END_ID'
        elif h == 'scan_date':
            yield 'scan_date:date'
        elif h == 'scan_dates':
            yield 'scan_date:date[]'
        elif h in ['server_name_indicators', 'locations', 'scan_src']:
            yield f'{h}:string[]'
        elif h in ['basicConstraints', 'isCa', 'valid']:
            yield f'{h}:boolean'
        elif h in ['inDegree', 'outDegree', 'numAltNames']:
            yield f'{h}:int'
        else:
            yield h

def from_neo4j_header(header: Iterable[str]):
    for h in header:
        if h == ':ID':
            yield 'id'
        elif h == ':START_ID':
            yield 'src'
        elif h == ':END_ID':
            yield 'dst'
        elif ':' in h:
            i = h.find(':')
            yield h[:i]
        else:
            yield h
