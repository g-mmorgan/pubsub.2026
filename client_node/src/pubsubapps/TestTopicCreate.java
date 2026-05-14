// ejemplo de manejo dinámico de temas con notificación
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

class TestTopicCreate {
    static public void main(String args[]) {
        if (args.length<3) {
            System.err.println("Usage: TestTopicCreate registryHost registryPort topic...");
            return;
        }
        try {
            PubSub srv = Client.init(args[0], args[1]);
            for (int i=2; i<args.length; i++) {
                String topic = args[i];
                srv.createTopic(topic); 
		System.out.println("Creado tema " + topic);
                TimeUnit.SECONDS.sleep(3);
            }
            for (int i=2; i<args.length; i++) {
                String topic = args[i];
                srv.deleteTopic(topic);
		System.out.println("Eliminado tema " + topic);
                TimeUnit.SECONDS.sleep(3);
            }
        } catch (Exception e) {
            e.printStackTrace();
	    System.exit(1);
        }
    }
}
