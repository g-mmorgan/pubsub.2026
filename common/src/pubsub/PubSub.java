// Interfaz remota de pubsub
// NO MODIFICAR
package pubsub;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Collection;

public interface PubSub extends Remote {
    public static final int version=1;
    int getVersion() throws RemoteException;
    boolean createTopic(String topic) throws RemoteException;
    Collection<String> topicList() throws RemoteException;
    boolean publish(Event ev) throws RemoteException;
    Event consumeEvent(String topic) throws RemoteException;
    Subscriber initSubscriber(SubscriberCallback c) throws RemoteException;
    Collection<Subscriber> subscriberList() throws RemoteException;
    Collection<Subscriber> subscriberListByTopic(String topic) throws RemoteException;
    boolean deleteTopic(String topic) throws RemoteException;
}
