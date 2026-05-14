// Clase que implementa la interfaz remota Subscriber
package broker;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import pubsub.Subscriber;
import pubsub.SubscriberCallback;
import pubsub.Event;

class SubscriberImpl extends UnicastRemoteObject implements Subscriber  {
    public static final long serialVersionUID = 1234567890L;

    UUID subUUID; // para facilitar depuración
    PubSubImpl ps; // para acceder a funcionalidad del servicio general

    // para notificar al subscriptor de creación y destrucción de temas
    transient SubscriberCallback scbk;

    private final Set<String> subscribedTopics;
    private final Queue<Event> eventQueue;
    private boolean finished;

    public SubscriberImpl(PubSubImpl p, SubscriberCallback s) throws RemoteException {
        scbk = s;
        subUUID = UUID.randomUUID();
        ps = p;

        subscribedTopics = new LinkedHashSet<>();
        eventQueue = new LinkedList<>();
        finished = false;
    }

    private void checkAlive() throws NoSuchObjectException {
        if (finished) {
            throw new NoSuchObjectException("this subscriber has already finished");
        }
    }

    public synchronized UUID getUUID() throws RemoteException {
        checkAlive();
        return subUUID;
    }

    public synchronized int subscribe(String topic, boolean glob) throws RemoteException {
        checkAlive();

        if (!glob) {
            return subscribeOneTopic(topic) ? 1 : 0;
        }

        int count = 0;
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + topic);

        for (String existingTopic : ps.topicList()) {
            if (matcher.matches(Paths.get(existingTopic))) {
                if (subscribeOneTopic(existingTopic)) {
                    count++;
                }
            }
        }

        return count;
    }

    private boolean subscribeOneTopic(String topic) {
        if (subscribedTopics.contains(topic)) {
            return false;
        }

        boolean addedToTopic = ps.subscribeSubscriberToTopic(this, topic);

        if (!addedToTopic) {
            return false;
        }

        subscribedTopics.add(topic);
        return true;
    }

    public synchronized Event getEvent() throws RemoteException {
        checkAlive();
        return eventQueue.poll();
    }

    public synchronized Collection<String> topicListBySubscriber() throws RemoteException {
        checkAlive();
        return new ArrayList<>(subscribedTopics);
    }

    public synchronized boolean unsubscribe(String topic) throws RemoteException {
        checkAlive();

        if (!subscribedTopics.contains(topic)) {
            return false;
        }

        boolean removedFromTopic = ps.unsubscribeSubscriberFromTopic(this, topic);

        if (!removedFromTopic) {
            return false;
        }

        subscribedTopics.remove(topic);
        return true;
    }

    public synchronized void exit() throws RemoteException {
        checkAlive();

        ps.removeSubscriberCompletely(this);

        subscribedTopics.clear();
        eventQueue.clear();
        finished = true;

        try {
            UnicastRemoteObject.unexportObject(this, true);
        } catch (Exception e) {
            // Si ya no estuviera exportado, no hace falta hacer nada más.
        }
    }

    synchronized void enqueueEvent(Event ev) {
        if (!finished) {
            eventQueue.add(ev);
        }
    }

    synchronized void removeTopicLocally(String topic) {
        subscribedTopics.remove(topic);
    }

    void notifyTopicAdded(String topic) throws RemoteException {
        if (!finished && scbk != null) {
            scbk.topicAdded(topic);
        }
    }

    void notifyTopicRemoved(String topic) throws RemoteException {
        if (!finished && scbk != null) {
            scbk.topicRemoved(topic);
        }
    }
}