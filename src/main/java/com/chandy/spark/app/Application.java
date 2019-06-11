package com.chandy.spark.app;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.IntegerRDD;

/**
 * @Author: chandy1994
 * @Date: 2019/06/12 0:38
 */
public class Application {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-custom-RDD");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> javaRDD1 = new IntegerRDD(sparkContext.sc()).toJavaRDD();
        JavaRDD<Integer> javaRDD2 = new IntegerRDD(sparkContext.sc()).toJavaRDD();
        javaRDD1.union(javaRDD2).distinct().foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sparkContext.close();
    }
}
