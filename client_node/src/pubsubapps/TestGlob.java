// Ejemplo que muestra el uso de patrones glob en la subscripción
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
import pubsub.Subscriber;

class TestGlob {
    static public void main(String args[]) {
        if (args.length<4) {
            System.err.println("Usage: TestGlob registryHost registryPort glob_pattern topic...");
            return;
        }
        try {
            PubSub srv = Client.init(args[0], args[1]);
            for (int i=3; i<args.length; i++) {
                String topic = args[i];
                srv.createTopic(topic); 
		System.out.println("Creado tema " + topic);
            }
            // null: no desea notificaciones de creación/borrado de temas
            Subscriber sub = srv.initSubscriber(null);
            String pattern = args[2];
            sub.subscribe(pattern, true); // con glob
            System.out.println("Temas subscritos ");
            for (String t: sub.topicListBySubscriber())
                System.out.println(t);

        } catch (Exception e) {
            e.printStackTrace();
	    System.exit(1);
        }
    }
}
