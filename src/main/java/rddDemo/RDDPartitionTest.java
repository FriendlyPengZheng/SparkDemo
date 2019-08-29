package rddDemo;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

class MyPartitioner extends Partitioner{

	public MyPartitioner(int partitionNums){
		this.partitonNums = partitionNums;
	}
	private int partitonNums;
	@Override
	public int getPartition(Object key) {
		//定制分区
		/*if(key.toString().equals("135"))
			return 1;
		if(key.toString().equals("136"))
			return 2;
		if(key.toString().equals("137"))
			return 3;
		if(key.toString().equals("138"))
			return 4;
		else
			return 0;*/
		
		//hash分区
		return key.hashCode() % partitonNums;
	}

	@Override
	public int numPartitions() {
		return partitonNums;
	}
	
}

public class RDDPartitionTest {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
		.setMaster("local")
		.setAppName("pairRDD test");

		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<Integer, Integer>> pairs = new ArrayList<Tuple2<Integer,Integer>>();
		pairs.add(new Tuple2<Integer,Integer>(135,2));
		pairs.add(new Tuple2<Integer,Integer>(136,6));
		pairs.add(new Tuple2<Integer,Integer>(137,6));
		pairs.add(new Tuple2<Integer,Integer>(138,4));
		pairs.add(new Tuple2<Integer,Integer>(136,55));
		pairs.add(new Tuple2<Integer,Integer>(137,33));
		pairs.add(new Tuple2<Integer,Integer>(138,22));
		pairs.add(new Tuple2<Integer,Integer>(139,35));
		pairs.add(new Tuple2<Integer,Integer>(139,53));
		pairs.add(new Tuple2<Integer,Integer>(139,82));
		
		JavaPairRDD<Integer,Integer> input = sc.parallelizePairs(pairs);

		JavaPairRDD<Integer, Integer> part = input.partitionBy(new MyPartitioner(8));
		
		//System.out.println("partitions["+TaskContext.getPartitionId()+"]:"+part.collect());
		
		part.foreach(new VoidFunction<Tuple2<Integer,Integer>>() {
			
			@Override
			public void call(Tuple2<Integer, Integer> t) throws Exception {
				/*
				 * TaskContext.getPartitionId()
				 * 获取当前分区号
				 */
				System.out.println("partitions["+TaskContext.getPartitionId()+"]:"+t._1()+" "+t._2());
			}
		});
		
		sc.close();
	}
	

	
}
