package fileDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkFileText {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
				.setAppName("file test")
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/*
		 * wholeTextFile()
		 * 只能从HDFS上读取文件
		 */
		JavaPairRDD<String, String> input = sc.wholeTextFiles("");
		
		System.out.println(input.collect());
		
		
		sc.close();
	}

}
