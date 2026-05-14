// clase Topic
package broker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import pubsub.Event;

class Topic {
    private final Queue<Event> eventQueue;
    private final Set<SubscriberImpl> subscribers;

    public Topic() {
        eventQueue = new LinkedList<>();
        subscribers = new LinkedHashSet<>();
    }

    synchronized void addEvent(Event ev) {
        eventQueue.add(ev);
    }

    synchronized Event consumeEvent() {
        return eventQueue.poll();
    }

    synchronized boolean addSubscriber(SubscriberImpl subscriber) {
        return subscribers.add(subscriber);
    }

    synchronized boolean removeSubscriber(SubscriberImpl subscriber) {
        return subscribers.remove(subscriber);
    }

    synchronized Collection<SubscriberImpl> subscriberList() {
        return new ArrayList<>(subscribers);
    }

    synchronized void clearSubscribers() {
        subscribers.clear();
    }
}