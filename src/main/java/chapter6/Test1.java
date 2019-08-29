package chapter6;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.AccumulatorV2;

public class Test1 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("test1")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//	E:\\MyDownloads/spark.txt
		JavaRDD<String> input = sc.textFile("E:\\MyDownloads/spark.txt");
		final MyAccumulator accu = new MyAccumulator();
		sc.sc().register(accu, "blankLines");
		//final Accumulator<Integer> blankLines = sc.accumulator(0);
		JavaRDD<String> callSigns = input.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String t) throws Exception {
				if(t.equals("")){//记录空行行数
					accu.add(1);
				}
				return Arrays.asList(t.split(" ")).iterator();
			}
		});
		
		System.out.println("callSigns="+callSigns.collect());
		System.out.println(accu.getNum());
		//System.out.println("lines="+blankLines.value());
	}

}
/*
 * 自定义累加器
 * 实现抽象类 	 AccumulatorV2
 */
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
