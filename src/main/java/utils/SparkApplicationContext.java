package utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkApplicationContext {
    private static JavaSparkContext jsc;
    static {
        SparkConf conf = new SparkConf().setAppName("Elastic").setMaster("local[*]");
        conf.set("es.index.auto.create", "true");
        jsc = new JavaSparkContext(conf);}
    public static JavaSparkContext getContext(){
        return jsc;
    }
}
