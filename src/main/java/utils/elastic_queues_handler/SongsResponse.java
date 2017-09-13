package utils.elastic_queues_handler;

import org.apache.spark.api.java.JavaPairRDD;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import utils.SparkApplicationContext;

import java.util.Map;

public class SongsResponse {
    private String taget;
    private String querryMatchPattern="{ \"query\" : {\"match\": {\"lyrics\": {\"query\":\"VALUE\",\"fuzziness\": \"AUTO\",\"operator\":  \"and\"}}}}";

    public SongsResponse(String taget) {
        this.taget = taget;
    }

    public JavaPairRDD<String, Map<String, Object>> getSongByPhraseFromLyrics(String phrase){
        return JavaEsSpark.esRDD(SparkApplicationContext.getContext(), taget,querryMatchPattern.replaceFirst("VALUE",phrase));
    }

}
