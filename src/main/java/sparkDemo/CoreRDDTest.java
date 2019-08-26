package sparkDemo;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.Test;

import sparkDemo.CoreRDDTest.AvgCount;


public class CoreRDDTest implements Serializable{

	@Test
	public void test1(){
		SparkConf conf = new SparkConf()
				.setAppName("test1")
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> input = sc.textFile("C:\\sparkInput/wordcount.txt");
		
		//第一种方式 	使用spark的标准java函数接口
		/*JavaRDD<String> result = input.filter(new Function<String, Boolean>() {
			
			public Boolean call(String v1) throws Exception {
				return v1.contains("hello");
			}
		});*/
		
		//第二种方式 	使用lambda表达式实现
		/*JavaRDD<String> result = input.filter(s -> s.contains("hello"));
		*/
		
		//第三种方式 	具体类进行函数传递
		/*JavaRDD<String> result = input.filter(new ContainsHello());
		*/
		//第四种方式 	带参数的java函数类
		JavaRDD<String> result = input.filter(new Contains("java"));
		
		for (String s : result.collect()) {
			System.out.println(s);
		}
		
		sc.close();
	}
	
	@Test
	public void test2(){
		SparkConf conf = new SparkConf()
				.setAppName("test2")
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> input = sc.parallelize(Arrays.asList(3,5,6,2,7,10,6,7));
		
		JavaRDD<Integer> result = input.map(new Function<Integer, Integer>() {

			@Override
			public Integer call(Integer v1) throws Exception {
				return v1*v1;
			}
			
		});
		
		JavaRDD<Integer> sort = result.sortBy(new Function<Integer, Integer>() {

			@Override
			public Integer call(Integer v1) throws Exception {
				return v1;
			}
			
		}, false, 1);
		
		//System.out.println(StringUtils.join(sort.collect(),","));
		
		sc.close();
	}
	
	@Test
	public void test3(){
		
		SparkConf conf = new SparkConf()
				.setAppName("test2")
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> input = sc.parallelize(Arrays.asList("abc dfg","hello world","java hadoop"));
		
		JavaRDD<String>  words= input.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" ")).iterator();
			}
		});
	
		System.out.println(words.collect());
		
		words.foreach(new VoidFunction<String>() {
			
			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
	}
	
	@Test
	public void test4(){
		SparkConf conf = new SparkConf()
				.setAppName("test2")
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> input = sc.parallelize(Arrays.asList(3));
		
		/*
		 * reduce()
		 */
		
		/*Integer result = input.reduce(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {		
				return v1 + v2;
			}
		});*/
		
		/*
		 * fold()
		 */
		Integer result = input.fold(0,new Function2<Integer, Integer, Integer>() {
			//加两次
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {		
				return v1 + v2;
			}
		});
		System.out.println(result);
	}
	/*
	 * aggregate()
	 */
	@Test
	public void test5(){
		SparkConf conf = new SparkConf()
				.setAppName("test2")
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> input = sc.parallelize(Arrays.asList(3,5,6,2,7,10,6,7));
		
		AvgCount result = input.aggregate(new AvgCount(0, 0),AvgCount.addAndCount , AvgCount.combine);
		
		System.out.println(result.avg());
		sc.close();
	}
	
	class ContainsHello implements Function<String, Boolean>{

		@Override
		public Boolean call(String v1) throws Exception {
			return v1.contains("hello");
		}
		
	}
	
	class Contains implements Function<String, Boolean>{

		private String name;
		
		public Contains(String name) {
			this.name = name;
		}
		@Override
		public Boolean call(String v1) throws Exception {
			return v1.contains(name);
		}	
	}
	
	static class AvgCount implements Serializable{
		private static int total;
		private static int cnt;
		
		public AvgCount(int total,int cnt){
			this.total = total;
			this.cnt = cnt;
		}

		static Function2<AvgCount,Integer,AvgCount> addAndCount = 
				new Function2<CoreRDDTest.AvgCount, Integer, CoreRDDTest.AvgCount>() {
			
			@Override
			public AvgCount call(AvgCount v1, Integer v2) throws Exception {
				v1.total +=v2;
				v1.cnt += 1;
				return v1;
			}
		};
		
		static Function2<AvgCount,AvgCount,AvgCount> combine = 
				new Function2<CoreRDDTest.AvgCount, CoreRDDTest.AvgCount, CoreRDDTest.AvgCount>() {
					
					@Override
					public AvgCount call(AvgCount v1, AvgCount v2) throws Exception {
						// TODO Auto-generated method stub
						v1.total += v2.total;
						v1.cnt += v2.cnt;
						return v1;
					}
				};
		static double avg(){
			return total/(double)cnt;
		}
	}
}
