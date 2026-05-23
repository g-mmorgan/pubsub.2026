// clase estatica que contacta con el Registry para obtener la
// referencia remota al servicio
package pubsubcln;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import pubsub.PubSub;

// no se puede instanciar ni derivar
public final class Client {
    // nombre del servicio registrado en el registry por el broker
    private static final String SERVICE_NAME = "PubSub";

    private Client() {}

    // localiza el registry en host:port y devuelve el stub del servicio
    // el stub es el objeto con el que el cliente habla de forma transparente
    static public PubSub init(String host, String port)
            throws RemoteException, NotBoundException {
        int registryPort = Integer.parseInt(port);
        // obtiene referencia al registry remoto
        Registry registry = LocateRegistry.getRegistry(host, registryPort);
        // busca el servicio por nombre y devuelve el stub
        return (PubSub) registry.lookup(SERVICE_NAME);
    }
}