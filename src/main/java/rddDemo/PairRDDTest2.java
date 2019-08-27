package rddDemo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PairRDDTest2 {

	public static void main(String[] args) {
		
		/*
		 * 自定义排序
		 */
		class MyComparator<Integer> implements Comparator<Integer>,Serializable{
			@Override
			public int compare(Integer o1, Integer o2) {
				//加一个-号表示倒序，默认为升序
				return -o1.toString().compareTo(o2.toString());
			}
		}
		
		SparkConf conf = new SparkConf()
		.setMaster("local")
		.setAppName("pairRDD test");

		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<Integer,Integer>> list = new ArrayList<Tuple2<Integer,Integer>>();
		list.add(new Tuple2<Integer,Integer>(1,2));
		list.add(new Tuple2<Integer,Integer>(4,6));
		list.add(new Tuple2<Integer,Integer>(3,4));
		list.add(new Tuple2<Integer,Integer>(3,6));
		
		JavaPairRDD<Integer,Integer> input = sc.parallelizePairs(list);
		
		JavaPairRDD<Integer,Integer> mySort = input.sortByKey(new MyComparator<Integer>());
		
		System.out.println("sort="+mySort.collect());
		
		/*
		 * countByKey()
		 * 分别对每一个key进行计数
		 */
		Map<Integer, Long> ctbk = input.countByKey();
		/*
		 * collectAsMap()
		 * 讲RDD转换为map
		 */
		Map<Integer, Integer> ctam = input.collectAsMap();
		/*
		 * lookup()
		 * 返回指定键的所有值
		 */
		List<Integer> lkup = input.lookup(3);
		System.out.println("ctam="+ctam);
		System.out.println("lkup="+lkup);
		System.out.println("ctbk="+ctbk);
		
		sc.close();
	}

}
