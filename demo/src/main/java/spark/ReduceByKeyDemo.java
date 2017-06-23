package spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ReduceByKeyDemo {

	public static void main(String[] args) {

		reduceByKeyDemo();
	}

	static void reduceByKeyDemo() {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("map");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<String> words = new ArrayList<String>();
		words.add("one");
		words.add("two");
		words.add("two");
		words.add("three");
		words.add("three");
		words.add("four");
		JavaRDD<String> wordRdd = sc.parallelize(words);
		JavaPairRDD<String, Integer> pairRDD = wordRdd.mapToPair(new PairFunction<String, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String value) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(value, 1);
			}
		});

		for (Tuple2<String, Integer> tuple : pairRDD.collect()) {
			System.out.println(tuple._1 + "@" + tuple._2());
		}
		JavaPairRDD<String, Integer> reducePairRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1 + v2;
			}
		});

		for (Tuple2<String, Integer> tuple : reducePairRDD.collect()) {
			System.out.println(tuple._1 + "@" + tuple._2);
			System.out.println("--------------");
		}
	}
}
