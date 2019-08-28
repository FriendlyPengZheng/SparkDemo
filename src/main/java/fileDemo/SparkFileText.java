package fileDemo;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;



import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

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
		JavaPairRDD<String, String> input = sc.wholeTextFiles("E:\\MyDownloads/spark.txt");
		
		JavaPairRDD<String, List<String>> result = input.mapValues(new Function<String, List<String>>() {

			@Override
			public List<String> call(String v1) throws Exception {
				return Arrays.asList(v1.split(" "));
			}
			
		});
		
		JavaRDD<List<String>> r = result.values();
		System.out.println(result.keys().collect());
		System.out.println(input.collect());
		System.out.println(result.collect());
		System.out.println(r.collect());
		
		sc.close();
	}

}
