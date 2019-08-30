package sparkSQL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlT1 {

	public static void main(String[] args) {

/*		SparkConf conf = new SparkConf()
						.setMaster("local")
						.setAppName("spark sql test");*/
		
		//JavaSparkContext sc = new JavaSparkContext(conf);
		
		//SparkSession spark = new SparkSession(sc.sc());
		
		/*
		 * 创建SparkSession
		 */
		SparkSession spark = SparkSession
				.builder()
				.appName("trans test")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> ds= spark.read().json("file/user.json");
		
		// Print the schema in a tree format
		ds.printSchema();

		ds.select("age").show();
		
		ds.groupBy("name").count().show();
		
		try {
			/*
			 * 创建视图
			 * Register the DataFrame as a SQL temporary view
			 */
			ds.createTempView("user");
		} catch (AnalysisException e) {
			e.printStackTrace();
		}
		/*
		 * 使用SQL方式查询
		 */
		spark.sql("select avg(age) as avg_name from user").show();
		
		spark.newSession().sql("select avg(age) as avg_name from user").show();
		//ds.show();
		
		//关闭资源
		spark.close();
		//sc.close();
	}

}
