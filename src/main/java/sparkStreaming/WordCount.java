package sparkStreaming;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
		.setMaster("local")
		.setAppName("SparkStreaming word count");
		
		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(3));
		
		JavaDStream<String> lineStream = sc.socketTextStream("10.1.1.34", 9999);
		
		JavaDStream<String> wordsStream = lineStream.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" ")).iterator();
			}
			
		});
		
		JavaPairDStream<String, Integer> wordsPairStream = wordsStream.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		});
		
		JavaPairDStream<String, Integer> result = wordsPairStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		result.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {
			
			@Override
			public void call(JavaPairRDD<String, Integer> t) throws Exception {
				System.out.println(t.take(10));
			}
		});
		System.out.println(result);
		//Start the computation
		sc.start();
		
		//Wait for the computation to terminate
		try {
			sc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
