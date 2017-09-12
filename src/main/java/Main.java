import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.opencsv.CSVReader;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import utils.CSVToElasticWrtiter;
import utils.Resources;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import  org.elasticsearch.spark.*;

import java.io.*;
import java.util.Map;

public class Main {
    private final static String file1="Lyrics1.csv";
    private final static String  file2="Lyrics2.csv";
    private final static  String fileDemo="LyricsDemo.csv";



    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("Elastic").setMaster("local[*]");
        conf.set("es.index.auto.create", "true");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        Resources resources = new Resources(fileDemo);
        CSVToElasticWrtiter csv = new CSVToElasticWrtiter(new BufferedReader(new FileReader(resources.getFile())),jsc);
        csv.writeIntoES();
        System.out.println("done");

    }
}