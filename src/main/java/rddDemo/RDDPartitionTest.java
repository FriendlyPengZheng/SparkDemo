package rddDemo;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.AccumulatorV2;

import scala.Tuple2;

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
		
		final MyAccumulator accu = new MyAccumulator();
		sc.sc().register(accu);
		
		JavaPairRDD<Integer, Integer> part = input.partitionBy(new MyPartitioner(8));
		
		//System.out.println("partitions["+TaskContext.getPartitionId()+"]:"+part.collect());
		
		part.foreach(new VoidFunction<Tuple2<Integer,Integer>>() {
			
			@Override
			public void call(Tuple2<Integer, Integer> t) throws Exception {
				/*
				 * TaskContext.getPartitionId()
				 * 获取当前分区号
				 */
				if(t._1().toString().equals("135")){
					accu.add(1);
				}
				System.out.println("partitions["+TaskContext.getPartitionId()+"]:"+t._1()+" "+t._2());
			}
		});
		System.out.println(accu.getNum());
		sc.close();
	}
	
}

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

class MyAccumulator extends AccumulatorV2<Integer, Integer>{

	private Integer num = 0;
	
	public Integer getNum() {
		return num;
	}

	public void setNum(Integer num) {
		this.num = num;
	}

	@Override
	public void add(Integer arg0) {
		num += arg0;
	}

	@Override
	public AccumulatorV2<Integer, Integer> copy() {
		MyAccumulator accu = new MyAccumulator();
		accu.setNum(num);
		return accu;
	}

	@Override
	public boolean isZero() {
		return num==0;
	}

	@Override
	public void merge(AccumulatorV2<Integer, Integer> arg0) {
		num += arg0.value();
	}

	@Override
	public void reset() {
		num =0;
	}

	@Override
	public Integer value() {
		return num;
	}
}
