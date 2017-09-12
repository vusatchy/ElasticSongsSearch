package utils;

import com.google.common.collect.ImmutableList;
import com.opencsv.CSVReader;
//import jersey.repackaged.com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.spark_project.guava.collect.ImmutableMap;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;


public class CSVToElasticWrtiter {
    private BufferedReader bfr;
    private JavaSparkContext jsc;

    public CSVToElasticWrtiter(BufferedReader bfr, JavaSparkContext jsc) {
        this.jsc=jsc;
        this.bfr = bfr;
    }

    public void  writeIntoES() throws IOException {

        CSVReader reader = null;
        reader = new CSVReader(bfr);
        String[] line;
        reader.readNext(); //names of feilds reading
        while ((line = reader.readNext()) != null) {
           /* for (String s:line
                 ) {
                System.out.println(s);
            }*/
            Map<String, String> data = ImmutableMap.of("band", line[0], "lyrics", line[1],"song",line[2]);
            JavaRDD<Map<String, ?>> javaRDD = jsc.parallelize(ImmutableList.of(data));
            JavaEsSpark.saveToEs(javaRDD, "elastic/songs");
        }
    }

}