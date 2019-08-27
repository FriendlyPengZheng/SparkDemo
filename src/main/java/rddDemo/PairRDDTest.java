package rddDemo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class PairRDDTest {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
				.setMaster("local")
				.setAppName("pairRDD test");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//JavaRDD<Integer> input = sc.parallelize(Arrays.asList(1,2,3,4,5));
		List<Tuple2<Integer,Integer>> list = new ArrayList<Tuple2<Integer,Integer>>();
		list.add(new Tuple2<Integer,Integer>(1,2));
		list.add(new Tuple2<Integer,Integer>(4,6));
		list.add(new Tuple2<Integer,Integer>(3,4));
		list.add(new Tuple2<Integer,Integer>(3,6));
		
		JavaPairRDD<Integer,Integer> input = sc.parallelizePairs(list);
		
		/*
		 * reduceByKey()
		 */
		JavaPairRDD<Integer,Integer> rdbk = input.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		System.out.println("rdbk=" + rdbk.collect());
		
		/*
		 * groupByKey()
		 */
		JavaPairRDD<Integer,Iterable<Integer>> gpbk = input.groupByKey(1);
		System.out.println(gpbk.take(2));
		
		/*
		 * mapValues()
		 */
		JavaPairRDD<Integer,String> mpv = input.mapValues(new Function<Integer, String>() {
			@Override
			public String call(Integer v1) throws Exception {
				return v1 + "abc";
			}
		});
		System.out.println(mpv.collect());
		
		/*
		 * flatMapValues()
		 */
		JavaPairRDD<Integer,String> ftmv = input.flatMapValues(new Function<Integer, Iterable<String>>() {
			@Override
			public Iterable<String> call(Integer v1) throws Exception {
				return Arrays.asList(v1 + "flat",v1 + "map",v1 + "values");
			}
		});
		System.out.println("ftmv="+ftmv.collect());
		
		/*
		 * keys()
		 */
		JavaRDD<Integer> ks = input.keys().distinct();
		System.out.println("ks="+ks.collect());
		
		/*
		 * values() 略
		 */
		
		/*
		 * sortByKey()
		 */
		JavaPairRDD<Integer,String> stbk = ftmv.sortByKey(false);
		System.out.println("stbk="+stbk.collect());
		
		sc.close();
	}
}
