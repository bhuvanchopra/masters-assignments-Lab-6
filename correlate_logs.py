import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re
line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('logs correlation').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
sc = spark.sparkContext

def parse(line):
    if line_re.match(line):
        return (line_re.match(line).group(1), line_re.match(line).group(4))
    else:
        return (None, None)
		
def main(inputs):
        	
    data = sc.textFile(inputs)
    request = data.map(parse)
    df_schema = types.StructType([
    types.StructField('hostname', types.StringType(), True),
    types.StructField('bytes', types.StringType(), True)])
	
    df = spark.createDataFrame(request, df_schema).dropna()
    df = df.withColumn('bytes', df['bytes'].cast(types.LongType()))
    df1 = df.groupBy('hostname').agg(functions.count('bytes').alias('x'))
    df2 = df.groupBy('hostname').agg(functions.sum('bytes').alias('y'))
    df_both = df1.join(df2, 'hostname').cache()
    df_final = df_both.select('hostname',functions.lit(1).alias('1'),'x','y',(df_both.x**2).alias('x2'),(df_both.y**2).alias('y2'),(df_both.x*df_both.y).alias('xy'))
    sums = df_final.groupBy().sum().cache()
    
    list1 = list(sums.first())
    r = ((list1[0]*list1[5]) - (list1[1]*list1[2]))/((((list1[0]*list1[3]) - list1[1]**2)**0.5)*(((list1[0]*list1[4]) - list1[2]**2)**0.5))
    print('r = %.6f' % r)
    print('r^2 = %.6f' % r**2)	

if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
