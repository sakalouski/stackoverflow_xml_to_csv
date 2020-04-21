import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._

val spark = SparkSession.builder.getOrCreate()
val df = spark.read.option("rowTag", "row").xml("./2019_dump/Badges.xml")
val selectedData = df.write.parquet("./2019_dump/Badges.parquet")
