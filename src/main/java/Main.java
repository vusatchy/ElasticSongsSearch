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
import utils.elastic_queues_handler.SongsResponse;

import java.io.*;
import java.util.Map;

public class Main {
    private final static String file1 = "Lyrics1.csv";
    private final static String  file2 = "Lyrics2.csv";
    private final static  String fileDemo  = "LyricsDemo.csv";
    private final static String target = "elastic/songs";

    public static void main(String[] args) throws IOException {
        Resources resources = new Resources(fileDemo);
        CSVToElasticWrtiter csv = new CSVToElasticWrtiter(resources.getFile(),target);
        //csv.writeIntoES();
        SongsResponse response = new SongsResponse(target);
        JavaPairRDD<String, Map<String, Object>> data=response.getSongByPhraseFromLyrics("air is sweter");
        data.foreach(x-> System.out.println(x));
        System.out.println("done");

    }
}