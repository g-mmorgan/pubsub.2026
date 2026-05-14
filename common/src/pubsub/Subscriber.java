// Interfaz remota de Subscriber
// NO MODIFICAR

package pubsub;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.UUID;

public interface Subscriber extends Remote {
    UUID getUUID() throws RemoteException;
    int subscribe(String topic, boolean glob) throws RemoteException;
    Event getEvent() throws RemoteException;
    Collection<String> topicListBySubscriber() throws RemoteException;
    boolean unsubscribe(String topic) throws RemoteException;
    void exit() throws RemoteException;
}
