// ejemplo de manejo dinámico de temas con notificación
package pubsubapps;
import java.util.Map;
import java.util.Date;
import java.util.Arrays;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import pubsubcln.Client;
import pubsub.PubSub;
import pubsub.Event;
import pubsub.Subscriber;
import pubsub.SubscriberCallback;

class TestTopicNotify extends UnicastRemoteObject implements SubscriberCallback {
    public static final long serialVersionUID=1234567890L;
    public TestTopicNotify () throws RemoteException {
    }
    public void topicAdded(String topic) throws RemoteException {
        System.out.println("Alta de tema " + topic);
    }
    public void topicRemoved(String topic) throws RemoteException {
        System.out.println("Baja de tema " + topic);
    }
    static public void main(String args[]) {
        if (args.length!=2) {
            System.err.println("Usage: TestTopicNotify registryHost registryPort");
            return;
        }
        try {
            PubSub srv = Client.init(args[0], args[1]);
            // desea notificaciones de creación/borrado de temas
            Subscriber sub = srv.initSubscriber(new TestTopicNotify());
            System.out.println("La lista inicial de temas es " + srv.topicList());
        } catch (Exception e) {
            e.printStackTrace();
	    System.exit(1);
        }
    }
}
