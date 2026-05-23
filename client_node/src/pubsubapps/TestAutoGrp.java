package pubsubapps;

// test automatico que cubre las 10 fases de la practica pubsub
// se ejecuta con: ./execute.sh TestAutoGrp localhost <puerto>
//
// cambios respecto a la version anterior:
//   - el catch de consumeEvent (fase 3) y getUUID tras exit (fase 9) ahora
//     captura RemoteException y busca NoSuchObjectException en la cadena de
//     causas, igual que hace Test.java del enunciado. el proxy en Client.java
//     que desenvolvia la excepcion ya no es necesario y debe eliminarse
//   - tests adicionales en todas las fases para mayor cobertura

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

    // contadores globales de tests pasados y fallados
    private static int passed = 0;
    private static int failed = 0;

    // lista de temas creados durante el test para limpiar al final
    private static final List<String> createdTopics = new ArrayList<>();

    // espera activa hasta maxMs milisegundos a que la condicion sea true
    // util para callbacks que llegan de forma asincrona por RMI
    private static boolean waitFor(java.util.function.BooleanSupplier condition, long maxMs) {
        long deadline = System.currentTimeMillis() + maxMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return true;
            try { Thread.sleep(50); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // un ultimo intento al agotar el tiempo
        return condition.getAsBoolean();
    }

    // implementacion de callback que registra los temas añadidos y eliminados
    // se usa en fases 4 y 10 para verificar las notificaciones
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

        // devuelve cuantos temas se han añadido hasta ahora
        public synchronized int addedCount() {
            return added.size();
        }

        // devuelve cuantos temas se han eliminado hasta ahora
        public synchronized int removedCount() {
            return removed.size();
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

    // ---------- helpers de eventos ----------

    // crea un evento con un unico campo numerico para facilitar la verificacion
    private static Event makeEvent(String topic, int n) {
        return new Event(topic, Map.of("numero de evento", n));
    }

    // verifica topic y numero de evento; falla si ev es null
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

    // verifica que ev es null (cola vacia o sin eventos)
    private static void checkNoEvent(String name, Event ev) {
        if (ev == null) ok(name);
        else            fail(name, "esperaba null pero llego: " + ev);
    }

    // ---------- helper: busca NoSuchObjectException en la cadena de causas ----------
    // RMI envuelve las excepciones del servidor en ServerException,
    // asi que hay que bajar por getCause() hasta encontrarla
    private static boolean isNoSuchObject(RemoteException e) {
        Throwable t = e;
        while (t != null) {
            if (t instanceof NoSuchObjectException) return true;
            t = t.getCause();
        }
        return false;
    }

    // ---------- helpers de creacion y busqueda ----------

    // crea un tema y lo añade a la lista de limpieza; registra fallo si no se crea
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

    // vacia la cola global de un tema usando consumeEvent en bucle
    private static void drainTopic(PubSub srv, String topic) throws RemoteException {
        while (srv.consumeEvent(topic) != null) {}
    }

    // borra todos los temas creados durante el test para no dejar basura en el broker
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

            // Client.init debe devolver el stub directamente sin proxy intermedio
            srv = Client.init(args[0], args[1]);
            check("srv no es null", srv != null, "Client.init devolvio null");
            checkEquals("getVersion devuelve 1", 1, srv.getVersion());

            // llamar getVersion varias veces debe ser estable
            checkEquals("getVersion segunda llamada devuelve 1", 1, srv.getVersion());


            // -------------------------------------------------------
            // fase 2: creacion y listado de temas
            // -------------------------------------------------------
            System.out.println();
            System.out.println("== fase 2: creacion y listado de temas ==");

            String t1 = base + "/tema1";
            String t2 = base + "/tema2";
            String t3 = base + "/tema3";

            // crear tres temas nuevos debe devolver true
            createTopicOrFail(srv, t1);
            createTopicOrFail(srv, t2);
            createTopicOrFail(srv, t3);

            // crear un tema que ya existe debe devolver false
            check("createTopic duplicado t1 devuelve false",
                    !srv.createTopic(t1), "crear tema repetido deberia devolver false");
            check("createTopic duplicado t2 devuelve false",
                    !srv.createTopic(t2), "crear tema repetido deberia devolver false");

            // topicList debe contener los tres temas creados
            Collection<String> topicListResult = srv.topicList();
            check("topicList no es null", topicListResult != null, "topicList devolvio null");
            check("topicList contiene t1", containsTopic(topicListResult, t1), "no contiene " + t1);
            check("topicList contiene t2", containsTopic(topicListResult, t2), "no contiene " + t2);
            check("topicList contiene t3", containsTopic(topicListResult, t3), "no contiene " + t3);

            // topicList debe ser una copia: modificarla no debe afectar al broker
            // (esto solo es verificable indirectamente; al menos comprobamos que no lanza)
            topicListResult.clear();
            check("topicList sigue devolviendo temas tras limpiar copia local",
                    srv.topicList().size() >= 3, "parece que topicList devolvio la lista interna");


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

            // consumir de un tema inexistente debe lanzar una excepcion cuya causa
            // sea NoSuchObjectException; RMI envuelve la excepcion en ServerException
            // por eso capturamos RemoteException y buscamos en la cadena de causas
            try {
                srv.consumeEvent(noExiste);
                fail("consumeEvent tema inexistente lanza excepcion",
                        "no lanzo ninguna excepcion");
            } catch (RemoteException e) {
                check("consumeEvent tema inexistente contiene NoSuchObjectException",
                        isNoSuchObject(e),
                        "la excepcion no contiene NoSuchObjectException: " + e);
            }

            // publicamos dos eventos en t1 y verificamos orden FIFO
            check("publish cola t1 evento 1", srv.publish(makeEvent(t1, 1)), "publish devolvio false");
            check("publish cola t1 evento 2", srv.publish(makeEvent(t1, 2)), "publish devolvio false");
            checkEvent("consumeEvent recibe evento 1 (FIFO)", srv.consumeEvent(t1), t1, 1);
            checkEvent("consumeEvent recibe evento 2 (FIFO)", srv.consumeEvent(t1), t1, 2);

            // la cola debe estar vacia ahora
            checkEquals("consumeEvent cola vacia devuelve null", null, srv.consumeEvent(t1));
            // segunda llamada en vacio tambien devuelve null
            checkEquals("consumeEvent segunda llamada en vacio devuelve null", null, srv.consumeEvent(t1));

            // publicar varios eventos en temas distintos; las colas son independientes
            check("publish t1 ev-A", srv.publish(makeEvent(t1, 10)), "publish devolvio false");
            check("publish t2 ev-B", srv.publish(makeEvent(t2, 20)), "publish devolvio false");
            check("publish t1 ev-C", srv.publish(makeEvent(t1, 11)), "publish devolvio false");

            // t1 debe tener 10 y 11 en ese orden; t2 debe tener 20
            checkEvent("consumeEvent t1 primer evento es 10", srv.consumeEvent(t1), t1, 10);
            checkEvent("consumeEvent t1 segundo evento es 11", srv.consumeEvent(t1), t1, 11);
            checkEquals("consumeEvent t1 vacia tras consumir", null, srv.consumeEvent(t1));
            checkEvent("consumeEvent t2 tiene 20", srv.consumeEvent(t2), t2, 20);
            checkEquals("consumeEvent t2 vacia tras consumir", null, srv.consumeEvent(t2));


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
            check("subscriberList no es null", subListResult != null, "subscriberList devolvio null");
            check("subscriberList contiene sub1",
                    containsSubscriber(subListResult, sub1uuid),
                    "sub1 no aparece en subscriberList");

            // subscriptor sin callback: su UUID debe ser distinto al de sub1
            Subscriber subSinCb = srv.initSubscriber(null);
            UUID subSinCbUuid = subSinCb.getUUID();
            check("subSinCb tiene UUID distinto a sub1",
                    !sub1uuid.equals(subSinCbUuid), "dos subscriptores no pueden tener el mismo UUID");

            // ambos deben aparecer en subscriberList
            Collection<Subscriber> subListDos = srv.subscriberList();
            check("subscriberList contiene sub1 con dos subscriptores",
                    containsSubscriber(subListDos, sub1uuid), "sub1 no aparece");
            check("subscriberList contiene subSinCb",
                    containsSubscriber(subListDos, subSinCbUuid), "subSinCb no aparece");

            // creamos un tema nuevo y esperamos el callback asincrono (hasta 2 segundos)
            String tCallback = base + "/callback";
            createTopicOrFail(srv, tCallback);
            boolean callbackOk = waitFor(() -> cb1.hasAdded(tCallback), 2000);
            check("callback topicAdded recibido para tCallback", callbackOk,
                    "no llego topicAdded en 2 segundos");

            // los temas creados ANTES de registrar sub1 no deben haber disparado callback
            check("callback NO recibido para t1 (creado antes del alta)",
                    !cb1.hasAdded(t1), "no deberia haber llegado topicAdded para t1");

            // subSinCb no tiene callback, crear otro tema no debe fallar aunque el otro
            // subscriptor tenga callback; verificamos creacion adicional
            String tCallback2 = base + "/callback2";
            createTopicOrFail(srv, tCallback2);
            boolean callbackOk2 = waitFor(() -> cb1.hasAdded(tCallback2), 2000);
            check("callback topicAdded recibido para tCallback2", callbackOk2,
                    "no llego topicAdded para tCallback2 en 2 segundos");


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

            // subscribirse a t1 de nuevo (ya subscrito) devuelve 0
            checkEquals("subscribe t1 repetido devuelve 0", 0,
                    sub1.subscribe(t1, false));

            // subscribirse a t2 devuelve 1
            checkEquals("subscribe t2 devuelve 1", 1,
                    sub1.subscribe(t2, false));

            // topicListBySubscriber debe contener t1 y t2
            Collection<String> sub1Topics = sub1.topicListBySubscriber();
            check("topicListBySubscriber sub1 contiene t1",
                    containsTopic(sub1Topics, t1), "no contiene " + t1);
            check("topicListBySubscriber sub1 contiene t2",
                    containsTopic(sub1Topics, t2), "no contiene " + t2);
            check("topicListBySubscriber sub1 NO contiene t3",
                    !containsTopic(sub1Topics, t3), "no deberia contener " + t3);

            // subscriberListByTopic de un tema inexistente debe devolver null
            checkEquals("subscriberListByTopic tema inexistente devuelve null",
                    null, srv.subscriberListByTopic(noExiste));

            // subscriberListByTopic de t1 debe contener sub1
            Collection<Subscriber> subsByT1 = srv.subscriberListByTopic(t1);
            check("subscriberListByTopic t1 no es null", subsByT1 != null,
                    "subscriberListByTopic devolvio null para t1 que existe");
            check("subscriberListByTopic t1 contiene sub1",
                    containsSubscriber(subsByT1, sub1uuid), "sub1 no aparece en t1");

            // subscriberListByTopic de t3 debe devolver lista vacia (existe pero sin subscriptores)
            Collection<Subscriber> subsByT3 = srv.subscriberListByTopic(t3);
            check("subscriberListByTopic t3 no es null (existe el tema)",
                    subsByT3 != null, "devolvio null para t3 que existe sin subscriptores");
            check("subscriberListByTopic t3 esta vacia",
                    subsByT3.isEmpty(), "t3 no deberia tener subscriptores aun");


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

            // antes de publicar, ninguno tiene eventos pendientes
            checkNoEvent("sub1 sin eventos antes de publicar", sub1.getEvent());
            checkNoEvent("sub2 sin eventos antes de publicar", sub2.getEvent());

            // publicamos en t1 y t2
            check("publish t1 evento 10", srv.publish(makeEvent(t1, 10)), "publish devolvio false");
            check("publish t2 evento 20", srv.publish(makeEvent(t2, 20)), "publish devolvio false");
            check("publish t1 evento 11", srv.publish(makeEvent(t1, 11)), "publish devolvio false");

            // sub1 esta subscrito a t1 y t2: recibe 10, 20 y 11 en ese orden de llegada
            checkEvent("sub1 recibe t1:10", sub1.getEvent(), t1, 10);
            checkEvent("sub1 recibe t2:20", sub1.getEvent(), t2, 20);
            checkEvent("sub1 recibe t1:11", sub1.getEvent(), t1, 11);
            checkNoEvent("sub1 sin mas eventos", sub1.getEvent());

            // sub2 tambien esta subscrito a t1 y t2: mismos eventos en el mismo orden
            checkEvent("sub2 recibe t1:10", sub2.getEvent(), t1, 10);
            checkEvent("sub2 recibe t2:20", sub2.getEvent(), t2, 20);
            checkEvent("sub2 recibe t1:11", sub2.getEvent(), t1, 11);
            checkNoEvent("sub2 sin mas eventos", sub2.getEvent());

            // publicar en t3 (nadie subscrito) no da eventos a nadie
            check("publish t3 evento 30", srv.publish(makeEvent(t3, 30)), "publish devolvio false");
            checkNoEvent("sub1 no recibe t3", sub1.getEvent());
            checkNoEvent("sub2 no recibe t3", sub2.getEvent());

            // vaciamos primero las colas globales acumuladas en t1 y t2 durante la fase
            drainTopic(srv, t1);
            drainTopic(srv, t2);
            drainTopic(srv, t3);

            // ahora publicamos un evento limpio para verificar la independencia de colas:
            // consumeEvent quita de la cola global del topic, pero los subscriptores
            // deben haber recibido igualmente el evento en su propia cola
            check("publish t1 ev 50 para verificar independencia de colas",
                    srv.publish(makeEvent(t1, 50)), "publish devolvio false");
            checkEvent("consumeEvent t1 saca 50 de cola global", srv.consumeEvent(t1), t1, 50);
            checkEvent("sub1 recibio t1:50 aunque lo consumi del topic",
                    sub1.getEvent(), t1, 50);
            checkEvent("sub2 recibio t1:50 aunque lo consumi del topic",
                    sub2.getEvent(), t1, 50);


            // -------------------------------------------------------
            // fase 7: subscripcion con patrones glob
            // -------------------------------------------------------
            System.out.println();
            System.out.println("== fase 7: subscripcion con glob ==");

            // estructura jerarquica de temas del enunciado
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

            // patron */tema[12]: un solo nivel intermedio, nombre tema1 o tema2
            // debe coincidir con g1 y g2 unicamente
            checkEquals("subGlob1 subscribe glob */tema[12] devuelve 2",
                    2, subGlob1.subscribe(base + "/nivel1a/*/tema[12]", true));

            // patron **/tema?: cualquier profundidad bajo nivel1a, nombre tema + 1 char
            // debe coincidir con g1, g2, g3, g4, g5 (5 temas); g6 no esta bajo nivel1a
            checkEquals("subGlob2 subscribe glob **/tema? devuelve 5",
                    5, subGlob2.subscribe(base + "/nivel1a/**/tema?", true));

            // verificamos que subGlob1 tiene exactamente g1 y g2
            Collection<String> glob1Topics = subGlob1.topicListBySubscriber();
            check("subGlob1 contiene g1", containsTopic(glob1Topics, g1), "falta " + g1);
            check("subGlob1 contiene g2", containsTopic(glob1Topics, g2), "falta " + g2);
            check("subGlob1 NO contiene g3 (no es tema[12])",
                    !containsTopic(glob1Topics, g3), "no deberia contener " + g3);
            check("subGlob1 NO contiene g4 (nivel mas profundo)",
                    !containsTopic(glob1Topics, g4), "no deberia contener " + g4);
            check("subGlob1 NO contiene g5 (nivel mas profundo)",
                    !containsTopic(glob1Topics, g5), "no deberia contener " + g5);
            check("subGlob1 NO contiene g6 (nivel1b distinto)",
                    !containsTopic(glob1Topics, g6), "no deberia contener " + g6);

            // verificamos que subGlob2 tiene g1 a g5 pero no g6
            Collection<String> glob2Topics = subGlob2.topicListBySubscriber();
            check("subGlob2 contiene g1", containsTopic(glob2Topics, g1), "falta " + g1);
            check("subGlob2 contiene g2", containsTopic(glob2Topics, g2), "falta " + g2);
            check("subGlob2 contiene g3", containsTopic(glob2Topics, g3), "falta " + g3);
            check("subGlob2 contiene g4", containsTopic(glob2Topics, g4), "falta " + g4);
            check("subGlob2 contiene g5", containsTopic(glob2Topics, g5), "falta " + g5);
            check("subGlob2 NO contiene g6 (bajo nivel1b no nivel1a)",
                    !containsTopic(glob2Topics, g6), "no deberia contener " + g6);

            // subscribe glob a patron que no matchea nada devuelve 0
            checkEquals("subscribe glob patron sin match devuelve 0",
                    0, subGlob1.subscribe(base + "/nada/**", true));

            // subscribe glob repetido sobre temas ya subscritos no debe duplicar
            // subGlob1 ya tiene g1 y g2; aplicar el mismo patron devuelve 0
            checkEquals("subscribe glob repetido sobre mismos temas devuelve 0",
                    0, subGlob1.subscribe(base + "/nivel1a/*/tema[12]", true));


            // -------------------------------------------------------
            // fase 8: baja de subscripcion (unsubscribe)
            // -------------------------------------------------------
            System.out.println();
            System.out.println("== fase 8: unsubscribe ==");

            // unsubscribe de un tema que no existe en el sistema -> false
            check("unsubscribe tema inexistente devuelve false",
                    !sub2.unsubscribe(noExiste),
                    "deberia devolver false para tema que no existe");

            // unsubscribe de un tema existente pero al que sub2 no esta subscrito -> false
            // sub2 esta en t1 y t2, pero no en t3
            check("unsubscribe tema existente sin subscripcion devuelve false",
                    !sub2.unsubscribe(t3),
                    "deberia devolver false para tema al que no esta subscrito");

            // unsubscribe de un tema al que si esta subscrito -> true
            check("unsubscribe t2 devuelve true",
                    sub2.unsubscribe(t2),
                    "deberia devolver true");

            // t2 ya no debe aparecer en la lista del subscriptor
            Collection<String> sub2TopicsAfter = sub2.topicListBySubscriber();
            check("sub2 ya no contiene t2 tras unsubscribe",
                    !containsTopic(sub2TopicsAfter, t2), "sigue conteniendo " + t2);

            // t1 todavia debe estar presente
            check("sub2 sigue conteniendo t1",
                    containsTopic(sub2TopicsAfter, t1), "no deberia haber perdido " + t1);

            // sub2 ya no debe aparecer en subscriberListByTopic de t2
            Collection<Subscriber> subsByT2After = srv.subscriberListByTopic(t2);
            check("t2 ya no contiene sub2 tras unsubscribe",
                    !containsSubscriber(subsByT2After, sub2uuid),
                    "sub2 sigue apareciendo en t2");

            // sub2 si debe seguir en t1
            Collection<Subscriber> subsByT1After = srv.subscriberListByTopic(t1);
            check("t1 sigue conteniendo sub2 tras unsubscribe de t2",
                    containsSubscriber(subsByT1After, sub2uuid),
                    "sub2 no deberia haber salido de t1");

            // unsubscribe repetido del mismo tema -> false (ya no esta subscrito)
            check("unsubscribe t2 repetido devuelve false",
                    !sub2.unsubscribe(t2),
                    "segundo unsubscribe del mismo tema deberia devolver false");

            // damos de baja t1 en sub2 y verificamos lista vacia
            check("unsubscribe t1 de sub2 devuelve true",
                    sub2.unsubscribe(t1),
                    "deberia devolver true");
            check("sub2 topicList vacia tras ambos unsubscribe",
                    sub2.topicListBySubscriber().isEmpty(),
                    "la lista deberia estar vacia");

            // publicar en t1 ahora no debe llegar a sub2
            check("publish t1 ev 99 para verificar que sub2 no recibe tras unsubscribe",
                    srv.publish(makeEvent(t1, 99)), "publish devolvio false");
            checkNoEvent("sub2 no recibe t1 tras unsubscribe", sub2.getEvent());
            // sub1 si lo recibe porque sigue subscrito
            checkEvent("sub1 sigue recibiendo t1 tras unsubscribe de sub2",
                    sub1.getEvent(), t1, 99);
            drainTopic(srv, t1);


            // -------------------------------------------------------
            // fase 9: fin de subscriptor (exit)
            // -------------------------------------------------------
            System.out.println();
            System.out.println("== fase 9: exit de subscriptor ==");

            // exit de sub2: a partir de aqui cualquier llamada debe lanzar excepcion
            // cuya causa sea NoSuchObjectException
            sub2.exit();

            // verificamos que sub2 ya no responde usando RemoteException + isNoSuchObject
            try {
                sub2.getUUID();
                fail("sub2.getUUID tras exit lanza excepcion", "no lanzo ninguna excepcion");
            } catch (RemoteException e) {
                check("sub2.getUUID tras exit contiene NoSuchObjectException",
                        isNoSuchObject(e),
                        "la excepcion no contiene NoSuchObjectException: " + e);
            }

            // tambien probamos que getEvent falla igual
            try {
                sub2.getEvent();
                fail("sub2.getEvent tras exit lanza excepcion", "no lanzo ninguna excepcion");
            } catch (RemoteException e) {
                check("sub2.getEvent tras exit contiene NoSuchObjectException",
                        isNoSuchObject(e),
                        "la excepcion no contiene NoSuchObjectException: " + e);
            }

            // sub2 no debe aparecer en la lista global de subscriptores
            check("sub2 no aparece en subscriberList tras exit",
                    !containsSubscriber(srv.subscriberList(), sub2uuid),
                    "sub2 sigue apareciendo en subscriberList tras exit");

            // sub2 no debe aparecer en ningun tema
            check("sub2 no aparece en t1 tras exit",
                    !containsSubscriber(srv.subscriberListByTopic(t1), sub2uuid),
                    "sub2 sigue en t1 tras exit");
            check("sub2 no aparece en t2 tras exit",
                    !containsSubscriber(srv.subscriberListByTopic(t2), sub2uuid),
                    "sub2 sigue en t2 tras exit");

            // simulamos subscriptor caido: exportamos un callback y luego lo tumbamos
            // para verificar que createTopic no explota aunque el callback falle
            Callback cbCaido = new Callback();
            srv.initSubscriber(cbCaido);
            // "tumbamos" el objeto remoto del callback para simular caida de red
            UnicastRemoteObject.unexportObject(cbCaido, true);

            // createTopic debe completarse sin excepcion aunque cbCaido este caido
            String tDespuesCaida = base + "/despues-caida";
            createTopicOrFail(srv, tDespuesCaida);
            ok("createTopic completa aunque hay subscriptor con callback caido");

            // sub1 (que sigue vivo y con callback) debe haber recibido la notificacion
            boolean notifDespuesCaida = waitFor(() -> cb1.hasAdded(tDespuesCaida), 2000);
            check("sub1 recibe topicAdded de tDespuesCaida aunque otro callback estaba caido",
                    notifDespuesCaida, "no llego topicAdded a sub1 en 2 segundos");


            // -------------------------------------------------------
            // fase 10: eliminacion de tema (deleteTopic)
            // -------------------------------------------------------
            System.out.println();
            System.out.println("== fase 10: deleteTopic ==");

            // deleteTopic de un tema que no existe debe devolver false
            check("deleteTopic inexistente devuelve false",
                    !srv.deleteTopic(noExiste),
                    "deberia devolver false");

            // sub1 esta subscrito a t1; deleteTopic t1 debe:
            //   1. devolver true
            //   2. sacar t1 de topicList
            //   3. sacar la subscripcion de sub1
            //   4. llamar topicRemoved en cb1
            check("deleteTopic t1 devuelve true",
                    srv.deleteTopic(t1),
                    "deberia devolver true");
            createdTopics.remove(t1); // ya borrado, no intentar de nuevo en cleanup

            // t1 ya no debe aparecer en topicList
            check("topicList ya no contiene t1",
                    !containsTopic(srv.topicList(), t1),
                    "t1 sigue apareciendo en topicList");

            // subscriberListByTopic de un tema eliminado debe devolver null
            checkEquals("subscriberListByTopic t1 eliminado devuelve null",
                    null, srv.subscriberListByTopic(t1));

            // sub1 debe haber perdido t1 de su lista de temas
            check("sub1 ya no contiene t1 tras deleteTopic",
                    !containsTopic(sub1.topicListBySubscriber(), t1),
                    "sub1 sigue con t1 en su lista");

            // el callback topicRemoved llega de forma asincrona: esperamos hasta 2 segundos
            boolean removeCallbackOk = waitFor(() -> cb1.hasRemoved(t1), 2000);
            check("callback topicRemoved recibido para t1", removeCallbackOk,
                    "no llego topicRemoved en 2 segundos");

            // deleteTopic del mismo tema ya eliminado devuelve false
            check("deleteTopic t1 segunda vez devuelve false",
                    !srv.deleteTopic(t1), "deberia devolver false al borrar un tema ya eliminado");

            // borramos t2 y comprobamos que la subscripcion de sub1 desaparece
            check("deleteTopic t2 devuelve true",
                    srv.deleteTopic(t2), "deberia devolver true");
            createdTopics.remove(t2);

            // sub1 no debe tener t2 en su lista
            check("sub1 ya no contiene t2 tras deleteTopic t2",
                    !containsTopic(sub1.topicListBySubscriber(), t2),
                    "sub1 sigue con t2");

            // notificacion topicRemoved para t2 debe haber llegado a cb1
            boolean removeT2Ok = waitFor(() -> cb1.hasRemoved(t2), 2000);
            check("callback topicRemoved recibido para t2", removeT2Ok,
                    "no llego topicRemoved de t2 en 2 segundos");

            // borramos t3
            check("deleteTopic t3 devuelve true",
                    srv.deleteTopic(t3), "deberia devolver true");
            createdTopics.remove(t3);

            // topicList ya no debe contener t1, t2 ni t3
            Collection<String> topicListFinal = srv.topicList();
            check("topicList no contiene t1", !containsTopic(topicListFinal, t1), "t1 sigue en topicList");
            check("topicList no contiene t2", !containsTopic(topicListFinal, t2), "t2 sigue en topicList");
            check("topicList no contiene t3", !containsTopic(topicListFinal, t3), "t3 sigue en topicList");

            // publicar en un tema recien eliminado debe devolver false
            check("publish en t1 eliminado devuelve false",
                    !srv.publish(makeEvent(t1, 999)),
                    "publish en tema eliminado deberia devolver false");


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
            // excepcion inesperada: la mostramos con stack trace completo para diagnosticar
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