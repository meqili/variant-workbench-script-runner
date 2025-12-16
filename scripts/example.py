import argparse
import glow
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser(description="Run the variant workbench script")

parser.add_argument("--input_file1",          type=str, help="Path to input file 1")
parser.add_argument("--input_file2",          type=str, help="Path to input file 2")
parser.add_argument("--input_folder1",        type=str, help="Path to input folder 1")
parser.add_argument("--input_folder2",        type=str, help="Path to input folder 2")
parser.add_argument("--input_tarred_file1",   type=str, help="Path to tarred input 1")
parser.add_argument("--input_tarred_file2",   type=str, help="Path to tarred input 2")

parser.add_argument("--clinvar",              action="store_true", help="Include ClinVar data")
parser.add_argument("--consequences",         action="store_true", help="Include consequences data")
parser.add_argument("--variants",             action="store_true", help="Include variants data")
parser.add_argument("--diagnoses",            action="store_true", help="Include diagnoses data")
parser.add_argument("--phenotypes",           action="store_true", help="Include phenotypes data")
parser.add_argument("--studies",              action="store_true", help="Include studies data")

parser.add_argument("--spark_executor_mem",         type=int, default=34,  help="Spark executor memory in GB")
parser.add_argument("--spark_executor_instance",    type=int, default=3,   help="Number of Spark executor instances")
parser.add_argument("--spark_executor_core",        type=int, default=5,   help="Number of Spark executor cores")
parser.add_argument("--spark_driver_maxResultSize", type=int, default=2,   help="Spark driver max result size in GB")
parser.add_argument("--sql_broadcastTimeout",       type=int, default=36000,help="Spark SQL broadcast timeout")
parser.add_argument("--spark_driver_core",          type=int, default=2,   help="Number of Spark driver cores")
parser.add_argument("--spark_driver_mem",           type=int, default=48,  help="Spark driver memory in GB")

args = parser.parse_args()

# Create SparkSession
spark = SparkSession \
    .builder \
    .appName('glow_pyspark') \
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.1.0,io.projectglow:glow-spark3_2.12:2.0.0') \
    .config('spark.jars.excludes', 'org.apache.hadoop:hadoop-client,io.netty:netty-all,io.netty:netty-handler,io.netty:netty-transport-native-epoll') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .config('spark.sql.debug.maxToStringFields', '0') \
    .config('spark.hadoop.io.compression.codecs', 'io.projectglow.sql.util.BGZFCodec') \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider') \
    .config('spark.kryoserializer.buffer.max', '512m') \
    .config('spark.executor.memory', f'{args.spark_executor_mem}G') \
    .config('spark.executor.instances', args.spark_executor_instance) \
    .config('spark.executor.cores', args.spark_executor_core) \
    .config('spark.driver.maxResultSize', f'{args.spark_driver_maxResultSize}G') \
    .config('spark.sql.broadcastTimeout', args.sql_broadcastTimeout) \
    .config('spark.driver.cores', args.spark_driver_core) \
    .config('spark.driver.memory', f'{args.spark_driver_mem}G') \
    .getOrCreate()
# Register so that glow functions like read vcf work with spark. Must be run in spark shell or in context described in help
spark = glow.register(spark)

# read s3 bucket
if args.clinvar:
    clinvar = spark.read.format("delta") \
        .load('s3a://kf-strides-public-vwb-prd/clinvar/')
if args.consequences:
    consequences = spark.read.format("delta") \
        .load('s3a://kf-strides-registered-vwb-prd/enriched/consequences')
if args.variants:
    variants = spark.read.format("delta") \
        .load('s3a://kf-strides-registered-vwb-prd/enriched/variants')
if args.diagnoses:
    diagnoses = spark.read.format("delta") \
        .load('s3a://kf-strides-registered-vwb-prd/enriched/disease')
if args.phenotypes:
    phenotypes = spark.read.format("delta") \
        .load('s3a://kf-strides-registered-vwb-prd/normalized/phenotype')
if args.studies:
    studies = spark.read.format("delta") \
        .load("s3a://kf-strides-registered-vwb-prd/normalized/research_study")

#############################################
######### MAIN ###############################
#############################################
from pyspark.sql import functions as F

# parameter configuration
input_file1 = args.input_file1

POT1_VUS = spark \
    .read \
    .format('csv') \
    .options(delimiter='\t', header=True, nullValue='-', nanValue='-') \
    .load(input_file1) \
    .withColumn('chromosome', F.regexp_replace(F.col('Chr'), '^chr', '')) \
    .drop('Chr') \
    .withColumnRenamed('Start', 'start') \
    .withColumnRenamed('REF', 'reference') \
    .withColumnRenamed('ALT', 'alternate')

cond = ['chromosome', 'start', 'reference', 'alternate']
output_filename = "POT1_VUS_output.tsv.gz"

POT1_VUS_output = POT1_VUS \
    .join(variants, on=cond, how='left')

POT1_VUS_output.toPandas().to_csv(output_filename, sep="\t", index=False, na_rep='-', compression='gzip')