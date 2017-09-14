import org.apache.spark.api.java.JavaPairRDD;
import utils.CSVToElasticWrtiter;
import utils.Resources;
import elastic_queues_handler.SongsResponse;

import java.io.*;
import java.util.Map;

public class Main {
    private final static String file1 = "Files/Lyrics1.csv";
    private final static String  file2 = "Files/Lyrics2.csv";
    private final static  String fileDemo  = "Files/LyricsDemo.csv";
    private final static String target = "pretest2/songs";

    public static void main(String[] args) throws IOException {

        Resources resources = new Resources(file1);
        CSVToElasticWrtiter csv = new CSVToElasticWrtiter(resources.getFile(),target);
        csv.writeIntoES();

        SongsResponse response = new SongsResponse(target);
        JavaPairRDD<String, Map<String, Object>> data=response.getSongByPhraseFromLyrics("air is sweter");
        data.foreach(x-> System.out.println(x));

    }
}