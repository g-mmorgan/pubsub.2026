// clase estática que contacta con el Registry para obtener la
// referencia remota al servicio

package pubsubcln;

import java.rmi.NotBoundException;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.ServerException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import pubsub.Event;
import pubsub.PubSub;
import pubsub.Subscriber;
import pubsub.SubscriberCallback;

// no se puede instanciar ni derivar
public final class Client {
    private static final String SERVICE_NAME = "PubSub";

    private Client(){}

    static public PubSub init(String host, String port)
            throws RemoteException, NotBoundException {

        int registryPort = Integer.parseInt(port);
        Registry registry = LocateRegistry.getRegistry(host, registryPort);
        PubSub remote = (PubSub) registry.lookup(SERVICE_NAME);

        return new PubSubWrapper(remote);
    }

    private static RemoteException unwrap(RemoteException e) {
        NoSuchObjectException noSuchObject = findNoSuchObjectException(e);

        if (noSuchObject != null) {
            return noSuchObject;
        }

        return e;
    }

    private static NoSuchObjectException findNoSuchObjectException(Throwable t) {
        while (t != null) {
            if (t instanceof NoSuchObjectException) {
                return (NoSuchObjectException) t;
            }

            t = t.getCause();
        }

        return null;
    }

    private static Subscriber wrapSubscriber(Subscriber subscriber) {
        if (subscriber == null) {
            return null;
        }

        if (subscriber instanceof SubscriberWrapper) {
            return subscriber;
        }

        return new SubscriberWrapper(subscriber);
    }

    private static Collection<Subscriber> wrapSubscribers(Collection<Subscriber> subscribers) {
        if (subscribers == null) {
            return null;
        }

        Collection<Subscriber> wrapped = new ArrayList<>();

        for (Subscriber subscriber : subscribers) {
            wrapped.add(wrapSubscriber(subscriber));
        }

        return wrapped;
    }

    private static class PubSubWrapper implements PubSub {
        private final PubSub remote;

        PubSubWrapper(PubSub remote) {
            this.remote = remote;
        }

        public int getVersion() throws RemoteException {
            try {
                return remote.getVersion();
            } catch (RemoteException e) {
                throw unwrap(e);
            }
        }

        public boolean createTopic(String topic) throws RemoteException {
            try {
                return remote.createTopic(topic);
            } catch (RemoteException e) {
                throw unwrap(e);
            }
        }

        public Collection<String> topicList() throws RemoteException {
            try {
                return remote.topicList();
            } catch (RemoteException e) {
                throw unwrap(e);
            }
        }

        public boolean publish(Event ev) throws RemoteException {
            try {
                return remote.publish(ev);
            } catch (RemoteException e) {
                throw unwrap(e);
            }
        }

        public Event consumeEvent(String topic) throws RemoteException {
            try {
                return remote.consumeEvent(topic);
            } catch (RemoteException e) {
                throw unwrap(e);
            }
        }

        public Subscriber initSubscriber(SubscriberCallback c) throws RemoteException {
            try {
                return wrapSubscriber(remote.initSubscriber(c));
            } catch (RemoteException e) {
                throw unwrap(e);
            }
        }

        public Collection<Subscriber> subscriberList() throws RemoteException {
            try {
                return wrapSubscribers(remote.subscriberList());
            } catch (RemoteException e) {
                throw unwrap(e);
            }
        }

        public Collection<Subscriber> subscriberListByTopic(String topic)
                throws RemoteException {
            try {
                return wrapSubscribers(remote.subscriberListByTopic(topic));
            } catch (RemoteException e) {
                throw unwrap(e);
            }
        }

        public boolean deleteTopic(String topic) throws RemoteException {
            try {
                return remote.deleteTopic(topic);
            } catch (RemoteException e) {
                throw unwrap(e);
            }
        }
    }

    private static class SubscriberWrapper implements Subscriber {
        private final Subscriber remote;

        SubscriberWrapper(Subscriber remote) {
            this.remote = remote;
        }

        public UUID getUUID() throws RemoteException {
            try {
                return remote.getUUID();
            } catch (RemoteException e) {
                throw unwrap(e);
            }
        }

        public int subscribe(String topic, boolean glob) throws RemoteException {
            try {
                return remote.subscribe(topic, glob);
            } catch (RemoteException e) {
                throw unwrap(e);
            }
        }

        public Event getEvent() throws RemoteException {
            try {
                return remote.getEvent();
            } catch (RemoteException e) {
                throw unwrap(e);
            }
        }

        public Collection<String> topicListBySubscriber() throws RemoteException {
            try {
                return remote.topicListBySubscriber();
            } catch (RemoteException e) {
                throw unwrap(e);
            }
        }

        public boolean unsubscribe(String topic) throws RemoteException {
            try {
                return remote.unsubscribe(topic);
            } catch (RemoteException e) {
                throw unwrap(e);
            }
        }

        public void exit() throws RemoteException {
            try {
                remote.exit();
            } catch (RemoteException e) {
                throw unwrap(e);
            }
        }
    }
}