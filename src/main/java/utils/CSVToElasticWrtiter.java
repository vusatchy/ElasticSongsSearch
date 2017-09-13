package utils;

import com.google.common.collect.ImmutableList;
import com.opencsv.CSVReader;
//import jersey.repackaged.com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.spark_project.guava.collect.ImmutableMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;


public class CSVToElasticWrtiter {
    private File file;
    private String target;

    public CSVToElasticWrtiter(File file,String target) {
        this.file = file;
        this.target=target;
    }

    public void  writeIntoES() throws IOException {

        CSVReader reader = null;
        reader = new CSVReader(new BufferedReader(new FileReader(file)));
        String[] line;
        reader.readNext(); //names of feilds readi
        while ((line = reader.readNext()) != null) {

            Map<String, String> data = ImmutableMap.of("band", line[0], "lyrics", line[1],"song",line[2]);
            JavaRDD<Map<String, ?>> javaRDD = SparkApplicationContext.getContext().parallelize(ImmutableList.of(data));
            JavaEsSpark.saveToEs(javaRDD, target);
        }
    }

}