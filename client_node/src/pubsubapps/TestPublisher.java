// ejemplo de editor
package pubsubapps;
import java.util.Map;
import java.util.Date;
import java.util.Arrays;
import java.util.UUID;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.TimeUnit;
import pubsubcln.Client;
import pubsub.PubSub;
import pubsub.Event;

class TestPublisher {
    static public void main(String args[]) {
        if (args.length!=3) {
            System.err.println("Usage: TestPublisher registryHost registryPort topic");
            return;
        }
        try {
            PubSub srv = Client.init(args[0], args[1]);
	    String topic = args[2];
            UUID uuid = UUID.randomUUID();
            int counter=0;
	    String greetings = "hola", string = "";

            srv.createTopic(topic); // si ya existe, no hace nada
	    while (true) {
	        Map<String,Object> content = Map.of(
                    "campo1", string+=greetings, // un string
                    "campo2", ++counter, // un entero
                    "campo3", new Date(), // un objeto
                    "campo4", Arrays.asList(uuid, "adiós") // una lista
	        );
		System.out.println("Publicado evento");
	        srv.publish(new Event(topic, content));
                TimeUnit.SECONDS.sleep(3);
	    }
        } catch (Exception e) {
            e.printStackTrace();
	    System.exit(1);
        }
    }
}
