package pubsubapps;

// test automatico que cubre las 10 fases de la practica pubsub
// se ejecuta con: ./execute.sh TestAutoGrp localhost <puerto>

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

public class TestAutoGrp {

    // contadores globales de test pasados y fallados
    private static int passed = 0;
    private static int failed = 0;

    // lista de temas creados durante el test para limpiar al final
    private static final List<String> createdTopics = new ArrayList<>();

    // espera activa hasta maxMs milisegundos a que la condicion sea true
    // util para los callbacks que llegan de forma asincrona por RMI
    private static boolean waitFor(java.util.function.BooleanSupplier condition, long maxMs) {
        long deadline = System.currentTimeMillis() + maxMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return true;
            try { Thread.sleep(50); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // un ultimo intento tras agotar el tiempo
        return condition.getAsBoolean();
    }

    // implementacion de callback que registra los temas añadidos y eliminados
    // se usa en las fases 4 y 10 para verificar las notificaciones
    public static class Callback extends UnicastRemoteObject implements SubscriberCallback {
        private static final long serialVersionUID = 1234567890L;

        private final List<String> added   = new ArrayList<>();
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

    // ---------- helpers de asercion ----------

    private static void ok(String name) {
        passed++;
        System.out.println("[OK]   " + name);
    }

    private static void fail(String name, String msg) {
        failed++;
        System.out.println("[FAIL] " + name + " -> " + msg);
    }

    private static void check(String name, boolean condition, String msg) {
        if (condition) ok(name);
        else           fail(name, msg);
    }

    private static void checkEquals(String name, Object expected, Object actual) {
        boolean eq = (expected == null && actual == null)
                || (expected != null && expected.equals(actual));
        if (eq) ok(name);
        else    fail(name, "esperado <" + expected + "> pero recibido <" + actual + ">");
    }

    // crea un evento con un unico campo numerico para facilitar la verificacion
    private static Event makeEvent(String topic, int n) {
        return new Event(topic, Map.of("numero de evento", n));
    }

    // verifica que el evento tiene el topic y numero de evento esperados
    private static void checkEvent(String name, Event ev, String expectedTopic, int expectedNumber) {
        if (ev == null) {
            fail(name, "evento null");
            return;
        }
        Object value = ev.getContent().get("numero de evento");
        if (expectedTopic.equals(ev.getTopic()) && Integer.valueOf(expectedNumber).equals(value)) {
            ok(name);
        } else {
            fail(name, "evento inesperado: " + ev);
        }
    }

    // crea un tema y lo añade a la lista de limpieza; falla el test si no se crea
    private static void createTopicOrFail(PubSub srv, String topic) throws Exception {
        boolean result = srv.createTopic(topic);
        createdTopics.add(topic);
        check("createTopic " + topic, result, "no se pudo crear el tema");
    }

    private static boolean containsTopic(Collection<String> topics, String topic) {
        return topics != null && topics.contains(topic);
    }

    // busca un subscriptor por UUID en la coleccion usando getUUID() remoto
    private static boolean containsSubscriber(Collection<Subscriber> subscribers, UUID uuid)
            throws Exception {
        if (subscribers == null) return false;
        for (Subscriber s : subscribers) {
            try {
                if (uuid.equals(s.getUUID())) return true;
            } catch (RemoteException e) {
                // ignoramos subscriptores caidos durante la busqueda
            }
        }
        return false;
    }

    // borra todos los temas que se crearon durante el test
    private static void cleanup(PubSub srv) {
        System.out.println();
        System.out.println("== limpieza de temas creados ==");
        for (String topic : createdTopics) {
            try {
                srv.deleteTopic(topic);
                System.out.println("  borrado: " + topic);
            } catch (Exception e) {
                System.out.println("  no se pudo borrar " + topic + ": " + e);
            }
        }
    }

    // ---------------------------------------------------------------
    // metodo principal: ejecuta los tests de las 10 fases en orden
    // ---------------------------------------------------------------
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("uso: TestAutoGrp registryHost registryPort");
            System.exit(2);
        }

        PubSub srv = null;

        // prefijo unico por ejecucion para evitar colisiones con temas de otras pruebas
        String base = "/autotest-" + System.currentTimeMillis();

        try {
            // -------------------------------------------------------
            // fase 1: conexion al broker y getVersion
            // -------------------------------------------------------
            System.out.println("== fase 1: conexion al broker ==");
            srv = Client.init(args[0], args[1]);
            checkEquals("getVersion devuelve 1", 1, srv.getVersion());

            // -------------------------------------------------------
            // fase 2: creacion y listado de temas
            // -------------------------------------------------------
            System.out.println();
            System.out.println("== fase 2: creacion y listado de temas ==");

            String t1 = base + "/tema1";
            String t2 = base + "/tema2";
            String t3 = base + "/tema3";

            createTopicOrFail(srv, t1);
            createTopicOrFail(srv, t2);
            createTopicOrFail(srv, t3);

            // crear un tema que ya existe debe devolver false
            check("createTopic duplicado devuelve false",
                    !srv.createTopic(t1),
                    "crear un tema repetido deberia devolver false");

            // topicList debe contener los tres temas creados
            Collection<String> topicListResult = srv.topicList();
            check("topicList contiene t1", containsTopic(topicListResult, t1), "no contiene " + t1);
            check("topicList contiene t2", containsTopic(topicListResult, t2), "no contiene " + t2);
            check("topicList contiene t3", containsTopic(topicListResult, t3), "no contiene " + t3);

            // -------------------------------------------------------
            // fase 3: publicacion y consumo como cola de mensajes
            // -------------------------------------------------------
            System.out.println();
            System.out.println("== fase 3: publicacion y consumo modo cola ==");

            String noExiste = base + "/noexiste";

            // publicar en un tema que no existe debe devolver false
            check("publish tema inexistente devuelve false",
                    !srv.publish(makeEvent(noExiste, 0)),
                    "publish deberia devolver false");

            // consumir de un tema inexistente debe lanzar NoSuchObjectException
            try {
                srv.consumeEvent(noExiste);
                fail("consumeEvent tema inexistente lanza NoSuchObjectException",
                        "no lanzo excepcion");
            } catch (NoSuchObjectException e) {
                ok("consumeEvent tema inexistente lanza NoSuchObjectException");
            }

            // publicamos dos eventos y los consumimos en orden FIFO
            check("publish cola evento 1", srv.publish(makeEvent(t1, 1)), "publish devolvio false");
            check("publish cola evento 2", srv.publish(makeEvent(t1, 2)), "publish devolvio false");
            checkEvent("consumeEvent recibe evento 1", srv.consumeEvent(t1), t1, 1);
            checkEvent("consumeEvent recibe evento 2", srv.consumeEvent(t1), t1, 2);

            // la cola debe estar vacia ahora
            checkEquals("consumeEvent cola vacia devuelve null", null, srv.consumeEvent(t1));

            // -------------------------------------------------------
            // fase 4: alta de subscriptores y callback de creacion de temas
            // -------------------------------------------------------
            System.out.println();
            System.out.println("== fase 4: alta de subscriptores y callback topicAdded ==");

            // sub1 quiere recibir notificaciones de creacion/borrado de temas
            Callback cb1 = new Callback();
            Subscriber sub1 = srv.initSubscriber(cb1);
            UUID sub1uuid = sub1.getUUID();

            check("sub1 getUUID no es null", sub1uuid != null, "getUUID devolvio null");

            // sub1 debe aparecer en la lista global de subscriptores
            Collection<Subscriber> subListResult = srv.subscriberList();
            check("subscriberList contiene sub1",
                    containsSubscriber(subListResult, sub1uuid),
                    "sub1 no aparece en subscriberList");

            // creamos un tema nuevo y esperamos el callback asincrono (hasta 2 segundos)
            String tCallback = base + "/callback";
            createTopicOrFail(srv, tCallback);
            boolean callbackOk = waitFor(() -> cb1.hasAdded(tCallback), 2000);
            check("callback topicAdded recibido para tCallback", callbackOk,
                    "no llego topicAdded en 2 segundos");

            // los temas creados antes de registrar sub1 no deben haber disparado callback
            check("callback NO recibido para t1 (creado antes del alta)",
                    !cb1.hasAdded(t1),
                    "no deberia haber llegado topicAdded para t1");

            // -------------------------------------------------------
            // fase 5: subscripcion a tema
            // -------------------------------------------------------
            System.out.println();
            System.out.println("== fase 5: subscripcion a tema ==");

            // subscribirse a un tema que no existe debe devolver 0
            checkEquals("subscribe tema inexistente devuelve 0", 0,
                    sub1.subscribe(noExiste, false));

            // subscribirse a t1 por primera vez devuelve 1
            checkEquals("subscribe t1 primera vez devuelve 1", 1,
                    sub1.subscribe(t1, false));

            // subscribirse a t1 de nuevo (ya esta subscrito) devuelve 0
            checkEquals("subscribe t1 repetido devuelve 0", 0,
                    sub1.subscribe(t1, false));

            // topicListBySubscriber debe contener t1
            Collection<String> sub1Topics = sub1.topicListBySubscriber();
            check("topicListBySubscriber de sub1 contiene t1",
                    containsTopic(sub1Topics, t1), "no contiene " + t1);

            // subscriberListByTopic de un tema inexistente debe devolver null
            checkEquals("subscriberListByTopic tema inexistente devuelve null",
                    null, srv.subscriberListByTopic(noExiste));

            // subscriberListByTopic de t1 debe contener sub1
            Collection<Subscriber> subsByT1 = srv.subscriberListByTopic(t1);
            check("subscriberListByTopic t1 contiene sub1",
                    containsSubscriber(subsByT1, sub1uuid), "sub1 no aparece en t1");

            // -------------------------------------------------------
            // fase 6: publicacion modo editor/subscriptor
            // -------------------------------------------------------
            System.out.println();
            System.out.println("== fase 6: publicacion modo editor/subscriptor ==");

            // sub2 sin callback, subscrito a t1 y t2
            Subscriber sub2 = srv.initSubscriber(null);
            UUID sub2uuid = sub2.getUUID();
            checkEquals("sub2 subscribe t1", 1, sub2.subscribe(t1, false));
            checkEquals("sub2 subscribe t2", 1, sub2.subscribe(t2, false));

            // antes de publicar, ninguno tiene eventos
            checkEquals("sub1 sin eventos antes de publicar", null, sub1.getEvent());
            checkEquals("sub2 sin eventos antes de publicar", null, sub2.getEvent());

            // publicamos en t1 y t2
            check("publish t1 evento 10", srv.publish(makeEvent(t1, 10)), "publish devolvio false");
            check("publish t2 evento 20", srv.publish(makeEvent(t2, 20)), "publish devolvio false");
            check("publish t1 evento 11", srv.publish(makeEvent(t1, 11)), "publish devolvio false");

            // sub1 solo esta subscrito a t1: recibe 10 y 11, no recibe t2
            checkEvent("sub1 recibe t1:10", sub1.getEvent(), t1, 10);
            checkEvent("sub1 recibe t1:11", sub1.getEvent(), t1, 11);
            checkEquals("sub1 no recibe t2", null, sub1.getEvent());

            // sub2 esta subscrito a t1 y t2: recibe 10, 20 y 11 en ese orden
            checkEvent("sub2 recibe t1:10", sub2.getEvent(), t1, 10);
            checkEvent("sub2 recibe t2:20", sub2.getEvent(), t2, 20);
            checkEvent("sub2 recibe t1:11", sub2.getEvent(), t1, 11);
            checkEquals("sub2 sin mas eventos", null, sub2.getEvent());

            // publicar en t3 (nadie subscrito) no da eventos a nadie
            check("publish t3 evento 30", srv.publish(makeEvent(t3, 30)), "publish devolvio false");
            checkEquals("sub1 no recibe t3", null, sub1.getEvent());
            checkEquals("sub2 no recibe t3", null, sub2.getEvent());

            // vaciamos las colas globales de los temas para no interferir con lo que sigue
            while (srv.consumeEvent(t1) != null) {}
            while (srv.consumeEvent(t2) != null) {}
            while (srv.consumeEvent(t3) != null) {}

            // -------------------------------------------------------
            // fase 7: subscripcion con patrones glob
            // -------------------------------------------------------
            System.out.println();
            System.out.println("== fase 7: subscripcion con glob ==");

            // creamos la estructura de temas jerarquica del enunciado
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

            // patron /nivel1a/*/tema[12]: solo un nivel de profundidad, tema1 y tema2
            // debe coincidir con g1 y g2 (2 subscripciones)
            checkEquals("subGlob1 subscribe glob */tema[12] devuelve 2",
                    2, subGlob1.subscribe(base + "/nivel1a/*/tema[12]", true));

            // patron /nivel1a/**/tema?: cualquier profundidad bajo nivel1a, nombre tema + 1 char
            // debe coincidir con g1, g2, g3, g4, g5 (5 subscripciones); g6 no esta bajo nivel1a
            checkEquals("subGlob2 subscribe glob **/tema? devuelve 5",
                    5, subGlob2.subscribe(base + "/nivel1a/**/tema?", true));

            Collection<String> glob1Topics = subGlob1.topicListBySubscriber();
            check("subGlob1 contiene g1", containsTopic(glob1Topics, g1), "falta " + g1);
            check("subGlob1 contiene g2", containsTopic(glob1Topics, g2), "falta " + g2);
            check("subGlob1 NO contiene g3", !containsTopic(glob1Topics, g3),
                    "no deberia contener " + g3);
            check("subGlob1 NO contiene g4", !containsTopic(glob1Topics, g4),
                    "no deberia contener " + g4);
            check("subGlob1 NO contiene g6", !containsTopic(glob1Topics, g6),
                    "no deberia contener " + g6);

            Collection<String> glob2Topics = subGlob2.topicListBySubscriber();
            check("subGlob2 contiene g1", containsTopic(glob2Topics, g1), "falta " + g1);
            check("subGlob2 contiene g2", containsTopic(glob2Topics, g2), "falta " + g2);
            check("subGlob2 contiene g3", containsTopic(glob2Topics, g3), "falta " + g3);
            check("subGlob2 contiene g4", containsTopic(glob2Topics, g4), "falta " + g4);
            check("subGlob2 contiene g5", containsTopic(glob2Topics, g5), "falta " + g5);
            check("subGlob2 NO contiene g6", !containsTopic(glob2Topics, g6),
                    "no deberia contener " + g6);

            // -------------------------------------------------------
            // fase 8: baja de subscripcion (unsubscribe)
            // -------------------------------------------------------
            System.out.println();
            System.out.println("== fase 8: unsubscribe ==");

            // unsubscribe de un tema que no existe en el sistema -> false
            check("unsubscribe tema inexistente devuelve false",
                    !sub2.unsubscribe(noExiste),
                    "deberia devolver false");

            // unsubscribe de un tema existente pero al que sub2 no esta subscrito -> false
            // sub2 esta en t1 y t2, pero no en t3
            check("unsubscribe tema no subscrito devuelve false",
                    !sub2.unsubscribe(t3),
                    "deberia devolver false");

            // unsubscribe de un tema al que si esta subscrito -> true
            check("unsubscribe t2 devuelve true",
                    sub2.unsubscribe(t2),
                    "deberia devolver true");

            // t2 ya no debe aparecer en la lista del subscriptor
            Collection<String> sub2TopicsAfter = sub2.topicListBySubscriber();
            check("sub2 ya no contiene t2 tras unsubscribe",
                    !containsTopic(sub2TopicsAfter, t2),
                    "sigue conteniendo " + t2);

            // t1 todavia debe estar
            check("sub2 sigue conteniendo t1",
                    containsTopic(sub2TopicsAfter, t1),
                    "no deberia haber perdido " + t1);

            // sub2 ya no debe aparecer en subscriberListByTopic de t2
            Collection<Subscriber> subsByT2After = srv.subscriberListByTopic(t2);
            check("t2 ya no contiene sub2 tras unsubscribe",
                    !containsSubscriber(subsByT2After, sub2uuid),
                    "sub2 sigue apareciendo en t2");

            // damos de baja tambien t1 en sub2 y verificamos lista vacia
            check("unsubscribe t1 de sub2 devuelve true",
                    sub2.unsubscribe(t1),
                    "deberia devolver true");
            check("sub2 topicList vacia tras ambos unsubscribe",
                    sub2.topicListBySubscriber().isEmpty(),
                    "la lista deberia estar vacia");

            // -------------------------------------------------------
            // fase 9: fin de subscriptor (exit)
            // -------------------------------------------------------
            System.out.println();
            System.out.println("== fase 9: exit de subscriptor ==");

            // exit de sub2: a partir de aqui cualquier llamada debe lanzar NoSuchObjectException
            sub2.exit();

            try {
                sub2.getUUID();
                fail("usar sub2 tras exit lanza NoSuchObjectException", "no lanzo excepcion");
            } catch (NoSuchObjectException e) {
                ok("usar sub2 tras exit lanza NoSuchObjectException");
            }

            // sub2 no debe aparecer en la lista global de subscriptores
            check("sub2 no aparece en subscriberList tras exit",
                    !containsSubscriber(srv.subscriberList(), sub2uuid),
                    "sub2 sigue apareciendo tras exit");

            // sub2 no debe aparecer en el tema t1 (se dano de baja en unsubscribe, pero lo confirmamos)
            check("sub2 no aparece en t1 tras exit",
                    !containsSubscriber(srv.subscriberListByTopic(t1), sub2uuid),
                    "sub2 sigue en t1 tras exit");

            // simulamos un subscriptor caido: creamos un sub con callback, cerramos el cliente
            // y luego creamos un tema para verificar que el broker no explota
            // usamos un callback que exportamos pero cuya JVM "cae" llamando a unexportObject
            Callback cbCaido = new Callback();
            Subscriber subCaido = srv.initSubscriber(cbCaido);
            // "tumbamos" el objeto remoto del callback para simular caida de red
            UnicastRemoteObject.unexportObject(cbCaido, true);

            // createTopic debe completarse aunque el callback de cbCaido falle
            String tDespuesCaida = base + "/despues-caida";
            createTopicOrFail(srv, tDespuesCaida);
            ok("createTopic completa correctamente aunque hay subscriptor caido");

            // -------------------------------------------------------
            // fase 10: eliminacion de tema (deleteTopic)
            // -------------------------------------------------------
            System.out.println();
            System.out.println("== fase 10: deleteTopic ==");

            // deleteTopic de un tema que no existe debe devolver false
            check("deleteTopic inexistente devuelve false",
                    !srv.deleteTopic(noExiste),
                    "deberia devolver false");

            // sub1 esta subscrito a t1; despues de deleteTopic t1 debe perder esa subscripcion
            // y recibir el callback topicRemoved
            check("deleteTopic t1 devuelve true",
                    srv.deleteTopic(t1),
                    "deberia devolver true");
            createdTopics.remove(t1); // ya borrado, no intentar de nuevo en cleanup

            // t1 ya no debe aparecer en topicList
            check("topicList ya no contiene t1",
                    !containsTopic(srv.topicList(), t1),
                    "t1 sigue apareciendo en topicList");

            // subscriberListByTopic de un tema eliminado debe devolver null
            checkEquals("subscriberListByTopic t1 borrado devuelve null",
                    null, srv.subscriberListByTopic(t1));

            // sub1 debe haber perdido t1 de su lista de temas
            check("sub1 ya no contiene t1 tras deleteTopic",
                    !containsTopic(sub1.topicListBySubscriber(), t1),
                    "sub1 sigue con t1 en su lista");

            // el callback topicRemoved llega de forma asincrona: esperamos hasta 2 segundos
            boolean removeCallbackOk = waitFor(() -> cb1.hasRemoved(t1), 2000);
            check("callback topicRemoved recibido para t1", removeCallbackOk,
                    "no llego topicRemoved en 2 segundos");

            // borramos t2 y t3 para verificar mas casos de deleteTopic
            check("deleteTopic t2 devuelve true",
                    srv.deleteTopic(t2), "deberia devolver true");
            createdTopics.remove(t2);

            check("deleteTopic t3 devuelve true",
                    srv.deleteTopic(t3), "deberia devolver true");
            createdTopics.remove(t3);

            // topicList debe reflejar los borrados
            Collection<String> topicListFinal = srv.topicList();
            check("topicList no contiene t2", !containsTopic(topicListFinal, t2), "t2 sigue en topicList");
            check("topicList no contiene t3", !containsTopic(topicListFinal, t3), "t3 sigue en topicList");

            // -------------------------------------------------------
            // resumen final
            // -------------------------------------------------------
            System.out.println();
            System.out.println("== resultado final ==");
            System.out.println("tests OK:   " + passed);
            System.out.println("tests FAIL: " + failed);

            cleanup(srv);

            System.exit(failed > 0 ? 1 : 0);

        } catch (Exception e) {
            // excepcion no esperada: la mostramos y salimos con error
            failed++;
            System.out.println();
            System.out.println("[ERROR] excepcion inesperada durante el test:");
            e.printStackTrace(System.out);

            if (srv != null) cleanup(srv);

            System.out.println();
            System.out.println("tests OK:   " + passed);
            System.out.println("tests FAIL: " + failed);
            System.exit(1);
        }
    }
}