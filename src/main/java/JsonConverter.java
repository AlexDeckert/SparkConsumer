import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * Created by nicob on 12.11.2016.
 * maps json-strings to message objects and backwards
 */

public class JsonConverter {
    private static JsonConverter instance = new JsonConverter();

    private ObjectMapper mapper = new ObjectMapper();

    public static JsonConverter getInstance() {
        return instance;
    }

    private JsonConverter() {
    }

    public KafkaMessage getKafkaMessage(String jsonString){
        try {
            return mapper.readValue(jsonString, KafkaMessage.class); //convert json-String to kafka message
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

//    public String toJsonString(KafkaMessage message){
//        try {
//            return mapper.writeValueAsString(message); //convert message to json-String
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
}