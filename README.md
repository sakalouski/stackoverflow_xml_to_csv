# stackoverflow_xml_to_csv
An efficient tool for converting raw stackoverflow data dump into .csv format. The tool works XML of June 2018 data dump. The processing speed is around 50k rows/second. 

The data is available here: https://archive.org/details/stackexchange

### If You are going to process it further with apache spark, do not convert it into .CSV 
Read the data right from the .XML files using https://github.com/databricks/spark-xml. 

Then simply:
<code>
df = spark.read.format('com.databricks.spark.xml')
  .options(rowTag='row')
  .load('../stackexchange/xml/PostHistory.xml', schema = schema_postHist)
  .repartition(400)
</code>

### If this does not work, try parsing .XML with the current tool, then save as .parquet:

<code>
  df = sqlContext.read.csv('../stackexchange/Posts.csv').cache()
  df.write.parquet("../stackexchange/Posts.parquet")
</code>
