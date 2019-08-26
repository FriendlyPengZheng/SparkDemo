package RDDDemo;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
				.setAppName("friendly`s word count !")
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> line = sc.textFile("C:\\sparkInput/wordcount.txt");
		
		JavaRDD<String> words = line.flatMap(new FlatMapFunction<String, String>() {

			public Iterator<String> call(String s) throws Exception {	
				return Arrays.asList(s.split(" ")).iterator();
			}
			
		});

		JavaPairRDD<String, Integer> wordMap = words.mapToPair(new PairFunction<String, String , Integer>() {

			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2<String, Integer>(s, 1);
			}
			
		});
		
		JavaPairRDD<String, Integer> result = wordMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		result.foreach(new VoidFunction<Tuple2<String,Integer>>() {

			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1() + " " +t._2());
			}
			
		});
		
		//result.saveAsTextFile("C:\\sparkOutput/wordCountResult");

		//result.collect();
		
		sc.close();
	}

}
