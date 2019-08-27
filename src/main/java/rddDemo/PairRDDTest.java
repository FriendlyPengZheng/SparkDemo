package rddDemo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
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
		 * 与JavaRDD的map()对应
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
		 * 与JavaRDD的flatMap()对应
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
		 * distinct()对RDD去重
		 */
		JavaRDD<Integer> ks = input.keys().distinct();
		System.out.println("ks="+ks.collect());
		
		/*
		 * values() 略
		 */
		
		/*
		 * sortByKey()
		 * 无参数时默认为true(增序排序)
		 */
		JavaPairRDD<Integer,String> stbk = ftmv.sortByKey(false);
		System.out.println("stbk="+stbk.collect());
		
		/*
		 * subtractByKey()
		 * 删除rdd中与other中的key相同的元素
		 * 只看是否有相同key
		 * subtract		减掉,除去
		 */
		List<Tuple2<Integer,Integer>> listRDD = new ArrayList<Tuple2<Integer,Integer>>();
		
		listRDD.add(new Tuple2<Integer, Integer>(1, 2));
		listRDD.add(new Tuple2<Integer, Integer>(3, 4));
		listRDD.add(new Tuple2<Integer, Integer>(3, 6));
		
		JavaPairRDD<Integer,Integer> rdd = sc.parallelizePairs(listRDD);
		
		List<Tuple2<Integer,Integer>> listOther = new ArrayList<Tuple2<Integer,Integer>>();
		
		listOther.add(new Tuple2<Integer, Integer>(3, 9));
		
		JavaPairRDD<Integer,Integer> other = sc.parallelizePairs(listOther);
		System.out.println("rdd before subtract:"+rdd.collect());
		JavaPairRDD<Integer, Integer> rddSubtract = rdd.subtractByKey(other);
		System.out.println("rdd after subtract:"+rddSubtract.collect());
		
		/*
		 * join()
		 * 对两个RDD进行内连接
		 */
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> rddJoin = rdd.join(other);
		System.out.println("rddJoin="+rddJoin.collect());
		
		/*
		 * leftOuterJoin()
		 * 左外链接
		 */
		JavaPairRDD<Integer,Tuple2<Integer,Optional<Integer>>> leftOuterRDD = rdd.leftOuterJoin(other);
		System.out.println("leftOuterRDD="+leftOuterRDD.collect());
		
		/*
		 * rightOuterJoin()
		 * 右外连接
		 */
		JavaPairRDD<Integer,Tuple2<Optional<Integer>,Integer>> rightOuterRDD = rdd.rightOuterJoin(other);
		System.out.println("rightOuterRDD="+rightOuterRDD.collect());
		
		/*
		 * cogroup()
		 * 将两个RDD中相同的键的数据分组到一起
		 */
		JavaPairRDD<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> cgRDD = rdd.cogroup(other);
		System.out.println("cdRDD="+cgRDD.collect());
		
		/*
		 * 功能：筛选掉rdd中value值大于5的
		 */
		Function<Tuple2<Integer, Integer>, Boolean> fun = new Function<Tuple2<Integer,Integer>, Boolean>() {
			
			@Override
			public Boolean call(Tuple2<Integer, Integer> v1) throws Exception {
				// TODO Auto-generated method stub
				return v1._2() < 5;
			}
		};
		JavaPairRDD<Integer,Integer> result = rdd.filter(fun);
		System.out.println("rdd="+rdd.collect());
		System.out.println("result="+result.collect());
		
		sc.close();
	}
}
