import org.codehaus.jackson.annotate.JsonProperty;

import java.io.Serializable;

/**
 * Created by nicob on 12.11.2016.
 * message class for production line messages
 */

public class KafkaMessage implements Serializable {
    @JsonProperty("value")
    private Object value;
    @JsonProperty("orderNumber")
    private String orderNumber;
    @JsonProperty("booleanValue")
    private boolean booleanValue;
    @JsonProperty("intValue")
    private int intValue;
    @JsonProperty("doubleValue")
    private double doubleValue;
    @JsonProperty("status")
    private String status;
    @JsonProperty("itemName")
    private String itemName;
    @JsonProperty("timestamp")
    private long timeStamp;

    public String getStatus() {
        return status;
    }

    @Override
    public String toString() {
        return "Kafka-Message: " + itemName + "; value: " + value + "; booleanValue: " + booleanValue + "; intValue: " + intValue +
                "; doubleValue: " + doubleValue + "; status: " + status + "; orderNumber: " + orderNumber + "; timeStamp: " + timeStamp;
    }
}