// clase estática que contacta con el Registry para dar de alta el servicio
package broker;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import pubsub.PubSub;

// no se puede instanciar ni derivar
public final class Server {
    private static final String SERVICE_NAME = "PubSub";

    private Server(){}

    static void init(PubSub pubsub, String port) throws RemoteException {
        int registryPort = Integer.parseInt(port);
        Registry registry = LocateRegistry.getRegistry(registryPort);
        registry.rebind(SERVICE_NAME, pubsub);
        System.out.println("Servicio " + SERVICE_NAME + " registrado en puerto " + registryPort);
    }
}