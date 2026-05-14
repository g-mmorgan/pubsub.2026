// Servidor que implementa la interfaz remota PubSub
package broker;

import java.rmi.RemoteException;
import java.rmi.NoSuchObjectException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import pubsub.Event;
import pubsub.PubSub;
import pubsub.Subscriber;
import pubsub.SubscriberCallback;

class PubSubImpl extends UnicastRemoteObject implements PubSub  {
    public static final long serialVersionUID = 1234567890L;

    private final Map<String, Topic> topics;
    private final Collection<SubscriberImpl> subscribers;

    public PubSubImpl() throws RemoteException {
        topics = new HashMap<>();
        subscribers = new ArrayList<>();
    }

    public int getVersion() throws RemoteException { // ya programada
        return version;
    }

    public synchronized boolean createTopic(String topic) throws RemoteException {
        if (topics.containsKey(topic)) {
            return false;
        }

        topics.put(topic, new Topic());

        for (SubscriberImpl subscriber : new ArrayList<>(subscribers)) {
            try {
                subscriber.notifyTopicAdded(topic);
            } catch (RemoteException e) {
                // Un subscriptor/callback puede haberse caído.
                // El broker debe continuar notificando al resto.
            }
        }

        return true;
    }

    public synchronized Collection<String> topicList() throws RemoteException {
        return new ArrayList<>(topics.keySet());
    }

    public synchronized boolean publish(Event ev) throws RemoteException {
        if (ev == null || ev.getTopic() == null) {
            return false;
        }

        Topic topic = topics.get(ev.getTopic());

        if (topic == null) {
            return false;
        }

        // Modo cola global por tema.
        topic.addEvent(ev);

        // Modo editor/subscriptor: una copia lógica del evento a cada subscriptor.
        for (SubscriberImpl subscriber : topic.subscriberList()) {
            subscriber.enqueueEvent(ev);
        }

        return true;
    }

    public synchronized Event consumeEvent(String topic) throws RemoteException {
        Topic t = topics.get(topic);

        if (t == null) {
            throw new NoSuchObjectException("topic does not exist");
        }

        return t.consumeEvent();
    }

    public synchronized Subscriber initSubscriber(SubscriberCallback c) throws RemoteException {
        SubscriberImpl subscriber = new SubscriberImpl(this, c);
        subscribers.add(subscriber);
        return subscriber;
    }

    public synchronized Collection<Subscriber> subscriberList() throws RemoteException {
        return new ArrayList<Subscriber>(subscribers);
    }

    public synchronized Collection<Subscriber> subscriberListByTopic(String topic)
            throws RemoteException {

        Topic t = topics.get(topic);

        if (t == null) {
            return null;
        }

        return new ArrayList<Subscriber>(t.subscriberList());
    }

    public synchronized boolean deleteTopic(String topic) throws RemoteException {
        Topic t = topics.get(topic);

        if (t == null) {
            return false;
        }

        Collection<SubscriberImpl> topicSubscribers = t.subscriberList();

        // Cancelar la subscripción de todos los subscriptores asociados a ese tema.
        for (SubscriberImpl subscriber : topicSubscribers) {
            subscriber.removeTopicLocally(topic);
        }

        t.clearSubscribers();
        topics.remove(topic);

        // Notificar la eliminación a todos los subscriptores del sistema interesados.
        for (SubscriberImpl subscriber : new ArrayList<>(subscribers)) {
            try {
                subscriber.notifyTopicRemoved(topic);
            } catch (RemoteException e) {
                // Un subscriptor/callback puede haberse caído.
                // El broker debe continuar notificando al resto.
            }
        }

        return true;
    }

    synchronized boolean subscribeSubscriberToTopic(SubscriberImpl subscriber, String topic) {
        Topic t = topics.get(topic);

        if (t == null) {
            return false;
        }

        return t.addSubscriber(subscriber);
    }

    synchronized boolean unsubscribeSubscriberFromTopic(SubscriberImpl subscriber, String topic) {
        Topic t = topics.get(topic);

        if (t == null) {
            return false;
        }

        return t.removeSubscriber(subscriber);
    }

    synchronized void removeSubscriberCompletely(SubscriberImpl subscriber) {
        for (Topic topic : topics.values()) {
            topic.removeSubscriber(subscriber);
        }

        subscribers.remove(subscriber);
    }

    static public void main (String args[])  {
        if (args.length != 1) {
            System.err.println("Usage: PubSubImpl registryPortNumber");
            return;
        }

        try {
            PubSub ps = new PubSubImpl();
            Server.init(ps, args[0]);
        } catch (Exception e) {
            System.err.println("PubSubImpl exception: " + e.toString());
            System.exit(1);
        }
    }
}