# stackoverflow_xml_to_csv
An efficient tool for converting raw stackoverflow data dump into .csv format. The tool works XML of June 2018 data dump. The processing speed is around 50k rows/second. 

The data is available here: https://archive.org/details/stackexchange

## If You are going to process it further with apache spark, do not convert it into .CSV 
Read right from .XML using https://github.com/databricks/spark-xml, then simply:

<code>
df = spark.read.format('com.databricks.spark.xml')
  .options(rowTag='row')
  .load('../stackexchange/xml/PostHistory.xml', schema = schema_postHist)
  .repartition(400)
</code>
