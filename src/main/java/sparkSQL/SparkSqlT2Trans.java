package sparkSQL;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class SparkSqlT2Trans {

	public static void main(String[] args) {
				
		//SparkSession spark = new SparkSession(sc.sc());
		SparkSession spark = SparkSession
				.builder()
				.appName("trans test")
				.master("local")
				.getOrCreate();
		/*
		 * SparkContext --> JavaSparkContext
		 */
		JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		
		Person person1 = new Person("张三",19);
		Person person2 = new Person("李四",23);
		Person person3 = new Person("王五",17);
		
		List<Person> personList = new ArrayList<Person>();
		personList.add(person1);
		personList.add(person2);
		personList.add(person3);

		JavaRDD<Person> rdd = sc.parallelize(personList);
	
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
	
		/*
		 * RDD<Person> --> DataSet<Person>
		 */
		Dataset<Person> dsPerson = spark.createDataset(rdd.rdd(), personEncoder);
		
		/*
		 * 创建DataSet
		 */
		Dataset<Person> ds = spark.createDataset(personList,personEncoder);
		
		/*
		 * RDD --> DataFrame 
		 */
		Dataset<Row> df = spark.createDataFrame(rdd, Person.class);
		
		/*df.show();
		df.printSchema();*/
		
		/*ds.printSchema();
		dsPerson.printSchema();
		ds.show();
		dsPerson.show();*/
	
		
	}

}

