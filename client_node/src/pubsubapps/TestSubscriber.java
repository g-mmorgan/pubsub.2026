// ejemplo de subscriptor
package pubsubapps;
import java.util.Map;
import java.util.Date;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import pubsubcln.Client;
import pubsub.PubSub;
import pubsub.Event;
import pubsub.Subscriber;

class TestSubscriber {
    static public void main(String args[]) {
        if (args.length<3) {
            System.err.println("Usage: TestSubscriber registryHost registryPort topic...");
            return;
        }
        try {
            PubSub srv = Client.init(args[0], args[1]);
	    // null: no desea notificaciones de creación/borrado de temas
	    Subscriber sub = srv.initSubscriber(null);
	    for (int i=2; i<args.length;i++) {
	        String topic = args[i];
                srv.createTopic(topic); // si ya existe, no hace nada
	        sub.subscribe(topic, false); // no glob
	    }
            while (true) {
	        Event ev;
	        System.out.println("---- inicio de recogida de eventos");
	        while ((ev=sub.getEvent())!=null)
	            System.out.println(ev);
	        System.out.println("---- fin de recogida de eventos");
                TimeUnit.SECONDS.sleep(5);
	    }
        } catch (Exception e) {
            e.printStackTrace();
	    System.exit(1);
        }
    }
}
