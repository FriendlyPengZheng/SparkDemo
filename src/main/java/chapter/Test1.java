package chapter;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class Test1 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("test1")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> input = sc.textFile("C:\\sparkInput/wordcount.txt");
		
		final Accumulator<Integer> blankLines = sc.accumulator(0);
		
		JavaRDD<String> callSigns = input.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String t) throws Exception {
				if(t.equals(""))//记录空行行数
					blankLines.add(1);
				return Arrays.asList(t.split(" ")).iterator();
			}
		});
		
		System.out.println("callSigns="+callSigns.collect());
		System.out.println("lines="+blankLines.value());
	}

}
