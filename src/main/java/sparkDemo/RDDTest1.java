package sparkDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class RDDTest1 {

	public static void main(String[] args) {
		
		
		SparkConf conf = new SparkConf()
				.setMaster("local")
				.setAppName("my test");

		//System.setProperty("hadoop.home.dir","C:\\Download");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> line = sc.textFile("C:\\sparkInput/test.txt");
		
		JavaRDD<String> helloRDD = line.filter(new Function<String, Boolean>() {
			
			public Boolean call(String v1) throws Exception {
				return v1.contains("hello");
			}
			
		});
		
		JavaRDD<String> javaRDD = line.filter(new Function<String, Boolean>() {
			
			public Boolean call(String v1) throws Exception {
				return v1.contains("java");
			}
			
		});
		
		JavaRDD<String> unionRDD = helloRDD.union(javaRDD);
		
	  /*unionRDD.foreach(new VoidFunction<String>() {
			
			public void call(String t) throws Exception {
				System.out.println(t);
			}
			
		});*/
		
		System.out.println("Input had " + unionRDD.count() + " concerning lines");
		
		for(String s : unionRDD.collect()/*take(100)*/){
			System.out.println(s);
		}
		
		//指定分区数 并保存到本地文件
		unionRDD.coalesce(1).saveAsTextFile("C:\\sparkOutput/result.txt");
		
		sc.close();
		
	}

}
