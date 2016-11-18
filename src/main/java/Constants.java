/**
 * Created by nicob on 18.11.2016.
 * collection of constant values
 */

public class Constants {

    public static final boolean TESTING = true;
    //are we on a windows machine
    public static final boolean WINDOWS_MACHINE = System.getProperty("os.name").toLowerCase().matches("(.*)windows(.*)");

    //kafka attributes
    public static final int KAFKA_PORT = 1001;
    public static final String KAFKA_TOPIC = "messageData";

    /**
     * checks the os and determines server address
     */
    public static String getServer() {
        return WINDOWS_MACHINE ? "192.168.99.100" : "127.0.0.1";
    }

}