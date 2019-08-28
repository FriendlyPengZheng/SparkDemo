package rddDemo;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

class MyPartitioner extends Partitioner{
	
	@Override
	public int numPartitions() {
		return 100;
	}
	
	@Override
	public int getPartition(Object arg0) {
		return 10;
	}
}

public class RDDPartitionTest {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
		.setMaster("local")
		.setAppName("pairRDD test");

		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<Integer, Integer>> pairs = new ArrayList<Tuple2<Integer,Integer>>();
		pairs.add(new Tuple2<Integer,Integer>(1,2));
		pairs.add(new Tuple2<Integer,Integer>(3,6));
		pairs.add(new Tuple2<Integer,Integer>(4,6));
		pairs.add(new Tuple2<Integer,Integer>(3,4));
		JavaPairRDD<Integer,Integer> input = sc.parallelizePairs(pairs);

		JavaPairRDD<Integer, Integer> part = input.partitionBy(new MyPartitioner());
		
		System.out.println(part.getNumPartitions());
		int a[] = {1,2};
		System.out.println(part.collectPartitions(a).length);
		
		sc.close();
	}
	

	
}
