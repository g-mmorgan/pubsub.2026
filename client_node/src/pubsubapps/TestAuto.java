package pubsubapps;

import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import pubsubcln.Client;
import pubsub.PubSub;
import pubsub.Subscriber;
import pubsub.SubscriberCallback;
import pubsub.Event;

public class TestAuto {
    private static int passed = 0;
    private static int failed = 0;
    private static final List<String> createdTopics = new ArrayList<>();

    /** Espera hasta maxMs milisegundos a que condition sea verdadera. */
    private static boolean waitFor(java.util.function.BooleanSupplier condition, long maxMs) {
        long deadline = System.currentTimeMillis() + maxMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return true;
            try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        return condition.getAsBoolean();
    }

    public static class Callback extends UnicastRemoteObject implements SubscriberCallback {
        private static final long serialVersionUID = 1234567890L;

        private final List<String> added = new ArrayList<>();
        private final List<String> removed = new ArrayList<>();

        public Callback() throws RemoteException {
            super();
        }

        public synchronized void topicAdded(String topic) throws RemoteException {
            added.add(topic);
            System.out.println("  [callback] topicAdded: " + topic);
        }

        public synchronized void topicRemoved(String topic) throws RemoteException {
            removed.add(topic);
            System.out.println("  [callback] topicRemoved: " + topic);
        }

        public synchronized boolean hasAdded(String topic) {
            return added.contains(topic);
        }

        public synchronized boolean hasRemoved(String topic) {
            return removed.contains(topic);
        }
    }

    private static void ok(String name) {
        passed++;
        System.out.println("[OK]   " + name);
    }

    private static void fail(String name, String msg) {
        failed++;
        System.out.println("[FAIL] " + name + " -> " + msg);
    }

    private static void check(String name, boolean condition, String msg) {
        if (condition) {
            ok(name);
        } else {
            fail(name, msg);
        }
    }

    private static void checkEquals(String name, Object expected, Object actual) {
        if ((expected == null && actual == null) || (expected != null && expected.equals(actual))) {
            ok(name);
        } else {
            fail(name, "esperado <" + expected + "> pero recibido <" + actual + ">");
        }
    }

    private static Event makeEvent(String topic, int n) {
        return new Event(topic, Map.of("número de evento", n));
    }

    private static void checkEvent(String name, Event ev, String expectedTopic, int expectedNumber) {
        if (ev == null) {
            fail(name, "evento null");
            return;
        }

        Object value = ev.getContent().get("número de evento");

        if (expectedTopic.equals(ev.getTopic()) && Integer.valueOf(expectedNumber).equals(value)) {
            ok(name);
        } else {
            fail(name, "evento inesperado: " + ev);
        }
    }

    private static void createTopicOrFail(PubSub srv, String topic) throws Exception {
        boolean result = srv.createTopic(topic);
        createdTopics.add(topic);
        check("createTopic " + topic, result, "no se pudo crear el tema");
    }

    private static boolean containsTopic(Collection<String> topics, String topic) {
        return topics != null && topics.contains(topic);
    }

    private static boolean containsSubscriber(Collection<Subscriber> subscribers, UUID uuid) throws Exception {
        if (subscribers == null) {
            return false;
        }

        for (Subscriber s : subscribers) {
            try {
                if (uuid.equals(s.getUUID())) {
                    return true;
                }
            } catch (RemoteException e) {
                // Ignoramos subscriptores caídos durante el test.
            }
        }

        return false;
    }

    private static void cleanup(PubSub srv) {
        System.out.println();
        System.out.println("== Limpieza de temas creados ==");

        for (String topic : createdTopics) {
            try {
                srv.deleteTopic(topic);
                System.out.println("  borrado: " + topic);
            } catch (Exception e) {
                System.out.println("  no se pudo borrar " + topic + ": " + e);
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Uso: TestAuto registryHost registryPort");
            System.exit(2);
        }

        PubSub srv = null;
        String base = "/autotest-" + System.currentTimeMillis();

        try {
            System.out.println("== Conexión al broker ==");
            srv = Client.init(args[0], args[1]);
            checkEquals("getVersion", 1, srv.getVersion());

            System.out.println();
            System.out.println("== Fase 2: creación y listado de temas ==");
            String t1 = base + "/tema1";
            String t2 = base + "/tema2";
            String t3 = base + "/tema3";

            createTopicOrFail(srv, t1);
            check("createTopic duplicado devuelve false", !srv.createTopic(t1), "crear un tema repetido debería devolver false");

            Collection<String> topicList = srv.topicList();
            check("topicList contiene tema creado", containsTopic(topicList, t1), "topicList no contiene " + t1);

            System.out.println();
            System.out.println("== Fase 3: publicación y consumo como cola ==");
            String noExiste = base + "/noexiste";

            check("publish sobre tema inexistente devuelve false", !srv.publish(makeEvent(noExiste, 0)), "publish debería devolver false");

            try {
                srv.consumeEvent(noExiste);
                fail("consumeEvent sobre tema inexistente lanza NoSuchObjectException", "no lanzó excepción");
            } catch (NoSuchObjectException e) {
                ok("consumeEvent sobre tema inexistente lanza NoSuchObjectException");
            }

            check("publish cola evento 1", srv.publish(makeEvent(t1, 1)), "publish devolvió false");
            check("publish cola evento 2", srv.publish(makeEvent(t1, 2)), "publish devolvió false");
            checkEvent("consumeEvent recibe evento 1", srv.consumeEvent(t1), t1, 1);
            checkEvent("consumeEvent recibe evento 2", srv.consumeEvent(t1), t1, 2);
            checkEquals("consumeEvent sin eventos devuelve null", null, srv.consumeEvent(t1));

            System.out.println();
            System.out.println("== Fase 4: alta de subscriptores y callback de creación ==");
            Callback cb1 = new Callback();
            Subscriber sub1 = srv.initSubscriber(cb1);
            UUID sub1uuid = sub1.getUUID();

            Collection<Subscriber> subscribers = srv.subscriberList();
            check("subscriberList contiene subscriptor creado", containsSubscriber(subscribers, sub1uuid), "no aparece el subscriptor");

            String tCallback = base + "/callback";
            createTopicOrFail(srv, tCallback);

            // El callback topicAdded llega por RMI de forma asíncrona: esperamos hasta 2s.
            boolean callbackReceived = waitFor(() -> cb1.hasAdded(tCallback), 2000);
            check("callback topicAdded recibido", callbackReceived, "no llegó topicAdded");

            System.out.println();
            System.out.println("== Fase 5: subscripción a tema ==");
            checkEquals("subscribe tema inexistente devuelve 0", 0, sub1.subscribe(noExiste, false));
            checkEquals("subscribe tema existente devuelve 1", 1, sub1.subscribe(t1, false));
            checkEquals("subscribe repetido devuelve 0", 0, sub1.subscribe(t1, false));

            Collection<String> sub1Topics = sub1.topicListBySubscriber();
            check("topicListBySubscriber contiene tema", containsTopic(sub1Topics, t1), "no contiene " + t1);

            Collection<Subscriber> subsByTopic = srv.subscriberListByTopic(t1);
            check("subscriberListByTopic contiene sub1", containsSubscriber(subsByTopic, sub1uuid), "sub1 no aparece en el tema");

            System.out.println();
            System.out.println("== Fase 6: publicación editor/subscriptor ==");
            // Creamos t2 antes de usarlo (no fue creado antes)
            createTopicOrFail(srv, t2);

            Subscriber sub2 = srv.initSubscriber(null);
            checkEquals("sub2 subscribe tema1", 1, sub2.subscribe(t1, false));
            checkEquals("sub2 subscribe tema2", 1, sub2.subscribe(t2, false));

            check("publish pubsub tema1", srv.publish(makeEvent(t1, 10)), "publish devolvió false");
            check("publish pubsub tema2", srv.publish(makeEvent(t2, 20)), "publish devolvió false");

            checkEvent("sub1 recibe evento tema1", sub1.getEvent(), t1, 10);
            checkEquals("sub1 no recibe tema2", null, sub1.getEvent());

            checkEvent("sub2 recibe evento tema1", sub2.getEvent(), t1, 10);
            checkEvent("sub2 recibe evento tema2", sub2.getEvent(), t2, 20);
            checkEquals("sub2 sin más eventos", null, sub2.getEvent());

            // Vaciamos la cola global de temas para que no interfiera con otros tests.
            while (srv.consumeEvent(t1) != null) {}
            while (srv.consumeEvent(t2) != null) {}

            System.out.println();
            System.out.println("== Fase 7: subscripción con glob ==");
            String g1 = base + "/nivel1a/nivel2a/tema1";
            String g2 = base + "/nivel1a/nivel2a/tema2";
            String g3 = base + "/nivel1a/nivel2a/tema3";
            String g4 = base + "/nivel1a/nivel2b/nivel3a/tema1";
            String g5 = base + "/nivel1a/nivel2b/nivel3a/tema2";
            String g6 = base + "/nivel1b/tema1";

            createTopicOrFail(srv, g1);
            createTopicOrFail(srv, g2);
            createTopicOrFail(srv, g3);
            createTopicOrFail(srv, g4);
            createTopicOrFail(srv, g5);
            createTopicOrFail(srv, g6);

            Subscriber subGlob1 = srv.initSubscriber(null);
            Subscriber subGlob2 = srv.initSubscriber(null);

            checkEquals("subscribe glob */tema[12]", 2, subGlob1.subscribe(base + "/nivel1a/*/tema[12]", true));
            checkEquals("subscribe glob **/tema?", 5, subGlob2.subscribe(base + "/nivel1a/**/tema?", true));

            Collection<String> glob1Topics = subGlob1.topicListBySubscriber();
            check("glob1 contiene g1", containsTopic(glob1Topics, g1), "falta " + g1);
            check("glob1 contiene g2", containsTopic(glob1Topics, g2), "falta " + g2);
            check("glob1 no contiene g3", !containsTopic(glob1Topics, g3), "no debería contener " + g3);

            System.out.println();
            System.out.println("== Fase 8: unsubscribe ==");
            check("unsubscribe tema inexistente devuelve false", !sub2.unsubscribe(noExiste), "debería devolver false");
            check("unsubscribe tema no subscrito devuelve false", !sub2.unsubscribe(t3), "debería devolver false");
            check("unsubscribe tema subscrito devuelve true", sub2.unsubscribe(t2), "debería devolver true");

            Collection<String> sub2Topics = sub2.topicListBySubscriber();
            check("sub2 ya no contiene tema2", !containsTopic(sub2Topics, t2), "sigue conteniendo " + t2);

            System.out.println();
            System.out.println("== Fase 9: exit de subscriptor ==");
            sub2.exit();

            try {
                sub2.getUUID();
                fail("usar subscriptor tras exit lanza NoSuchObjectException", "no lanzó excepción");
            } catch (NoSuchObjectException e) {
                ok("usar subscriptor tras exit lanza NoSuchObjectException");
            }

            System.out.println();
            System.out.println("== Fase 10: deleteTopic ==");
            check("deleteTopic inexistente devuelve false", !srv.deleteTopic(noExiste), "debería devolver false");

            check("deleteTopic existente devuelve true", srv.deleteTopic(t1), "debería devolver true");
            // Eliminamos t1 de la lista de limpieza para no intentar borrarlo dos veces
            createdTopics.remove(t1);

            check("topicList ya no contiene tema borrado", !containsTopic(srv.topicList(), t1), "sigue apareciendo " + t1);
            checkEquals("subscriberListByTopic tema borrado devuelve null", null, srv.subscriberListByTopic(t1));

            // El callback topicRemoved también llega de forma asíncrona: esperamos hasta 2s.
            boolean removeCallbackReceived = waitFor(() -> cb1.hasRemoved(t1), 2000);
            check("callback topicRemoved recibido", removeCallbackReceived, "no llegó topicRemoved");

            System.out.println();
            System.out.println("== Resultado final ==");
            System.out.println("Tests OK:   " + passed);
            System.out.println("Tests FAIL: " + failed);

            cleanup(srv);

            if (failed > 0) {
                System.exit(1);
            }

            System.exit(0);

        } catch (Exception e) {
            failed++;
            System.out.println();
            System.out.println("[ERROR] Excepción inesperada durante el test:");
            e.printStackTrace(System.out);

            if (srv != null) {
                cleanup(srv);
            }

            System.out.println();
            System.out.println("Tests OK:   " + passed);
            System.out.println("Tests FAIL: " + failed);
            System.exit(1);
        }
    }
}