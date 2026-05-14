// Interfaz remota de SubscriberCallback
// NO MODIFICAR

package pubsub;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface SubscriberCallback extends Remote {
    void topicAdded(String topic) throws RemoteException;
    void topicRemoved(String topic) throws RemoteException;
}
