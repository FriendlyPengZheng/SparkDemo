package rddDemo;

import java.util.Arrays;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

public class RDDPartitionTest {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
		.setMaster("local")
		.setAppName("pairRDD test");

		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> input = sc.parallelize(Arrays.asList(1,2,3,4,5));
		
		Optional<Partitioner> parts = input.partitioner();
		
		System.out.println(parts);
		
		sc.close();
	}

}
