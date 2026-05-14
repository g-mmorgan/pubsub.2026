// Ejemplo que usa el modo de operación de tipo cola
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

class TestConsumer {
    static public void main(String args[]) {
        if (args.length!=3) {
            System.err.println("Usage: TestSConsumerubscriber registryHost registryPort topic");
            return;
        }
        try {
            PubSub srv = Client.init(args[0], args[1]);
	    String topic = args[2];
            srv.createTopic(topic); // si ya existe, no hace nada
            while (true) {
	        Event ev;
	        System.out.println("---- inicio de recogida de eventos");
                while ((ev=srv.consumeEvent(topic))!=null)
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
