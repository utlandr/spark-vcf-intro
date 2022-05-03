from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    FloatType,
    StringType,
    StructType,
    StructField,
    MapType,
)
from pyspark.sql.functions import col, split, udf
import hashlib

VCF_FILE = "file:///home/richard/tmp/vcfread/HG002_CHD7_AD.vcf.gz"

spark = (
    SparkSession.builder.config("spark.sql.catalogImplementation", "hive")
    .config("spark.sql.crossJoin.enabled", "true")
    .getOrCreate()
)

sc = spark.sparkContext

vcf_schema = StructType([StructField('CHROM', StringType(), True),
                         StructField('POS', StringType(), True),
                         StructField('ID', StringType(), True),
                         StructField('REF', StringType(), True),
                         StructField('ALT', StringType(), True),
                         StructField('QUAL', StringType(), True),
                         StructField('FILTER', StringType(), True),
                         StructField('INFO', StringType(), True),
                         StructField('FORMAT', StringType(), True),
                         StructField('SAMPLE1', StringType(), True),
                         StructField('SAMPLE2', StringType(), True),
                         StructField('SAMPLE3', StringType(), True),
                         StructField('SAMPLE4', StringType(), True),
                         StructField('SAMPLE5', StringType(), True),
                        ])


vcf_df = spark.read.schema(vcf_schema).option("sep", "\t").csv(VCF_FILE).filter("CHROM not like '##%'")
vcf_header = vcf_df.filter("CHROM = '#CHROM'")
vcf_df = vcf_df.subtract(vcf_header)
vcf_header = vcf_header.first()

samples = list()

for column in vcf_df.columns:
    if column.startswith("SAMPLE"):
        if vcf_header[column]:
            vcf_df = vcf_df.withColumnRenamed(column, vcf_header[column])
            samples.append(vcf_header[column])
        else:
            vcf_df = vcf_df.drop(column)
vcf_df = vcf_df.withColumn("POS", col("POS").cast("integer")).withColumn("QUAL", col("QUAL").cast("double"))

vcf_df.printSchema()
vcf_df.show()

print("SAMPLES", samples)
for sample in ["FORMAT"] + samples:
    vcf_df = vcf_df.withColumn(sample, split(col(sample), ":"))

vcf_df.printSchema()
vcf_df.show()

    
@udf(MapType(StringType(), StringType()))
def create_dict(keys, values):
    res = dict()
    for index, key in enumerate(keys):
        res[key] = values[index]
    return res
for sample in samples:
    vcf_df = vcf_df.withColumn(sample, create_dict(col("FORMAT"), col(sample)))
    
vcf_df.printSchema()
vcf_df.show()

@udf(StringType())
def generate_hash(chrom, pos, ref, alt):
    return hashlib.md5("{}:{}:{}:{}".format(chrom, pos, ref, alt).encode()).hexdigest()

vcf_df = vcf_df.withColumn("variant_hash", generate_hash(col("CHROM"), col("POS"), col("REF"), col("ALT")))

vcf_df.printSchema()
vcf_df.show()
