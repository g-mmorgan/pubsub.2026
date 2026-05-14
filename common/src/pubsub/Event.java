// Clase que define un evento
// NO MODIFICAR
package pubsub;
import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;

public class Event implements Serializable {
    public static final long serialVersionUID=1234567890L;
    String topic;
    HashMap <String,Object> content;
    public Event(String t, Map <String,Object> c) {
        topic = t;
        content = new HashMap<>(c);
    }
    public String getTopic() {
        return topic;
    }
    public Map <String,Object> getContent() {
        return content;
    }
    public String toString() {
        return "topic = <" + topic + "> content = <" + content + ">";
    }
}
