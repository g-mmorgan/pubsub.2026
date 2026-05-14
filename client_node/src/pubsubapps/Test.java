// Programa para realizar las pruebas planteadas en el enunciado.
package pubsubapps;
import java.util.Scanner;
import java.util.Collection;
import java.util.ArrayList;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import java.rmi.NoSuchObjectException;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import pubsubcln.Client;
import pubsub.PubSub;
import pubsub.Event;
import pubsub.Subscriber;
import pubsub.SubscriberCallback;

class Test extends UnicastRemoteObject implements SubscriberCallback {
    public static final long serialVersionUID=1234567890L;
    static int nev = 0;
    static Subscriber subscriber;
    public Test () throws RemoteException {
    }
    public void topicAdded(String topic) throws RemoteException {
        System.out.println("Notificación de alta de tema " + topic);
    }
    public void topicRemoved(String topic) throws RemoteException {
        System.out.println("Notificación de baja de tema " + topic);
    }
    static private void prompt() {
        System.err.println("Introduzca operacion (Ctrl-D para terminar)");
        System.err.println("\tgetVersion|createTopic|topicList|publish|consumeEvent|initSubscriber|\n\tgetUUID|subscriberList|subscribe|subscriberListByTopic|getEvent|\n\tsubscribeGlob|topicListBySubscriber|unsubscribe|exit|deleteTopic");
    }
    static private boolean doGetVersion(PubSub srv,  Scanner ent) throws RemoteException {
        System.err.println("getVersion ha devuelto: " + srv.getVersion());
        return true;
    }
    static private boolean doCreateTopic(PubSub srv,  Scanner ent) throws RemoteException {
        System.err.println("Introduzca el nombre del tema");
        if (!ent.hasNextLine()) return false;
        String lin = ent.nextLine();
        Scanner s = new Scanner(lin);
        if  (!s.hasNext()) return false;
        String tname = s.next();
	if (srv.createTopic(tname))
            System.err.println("Se ha creado el tema");
	else 
            System.err.println("Error al crear el tema");
        return true;
    }
    static private boolean doTopicList(PubSub srv,  Scanner ent) throws RemoteException {
        System.out.println("Topic List ");
        for (String t: srv.topicList())
            System.out.print("  Topic " + t);
        System.out.println("");
        return true;
    }
    static private boolean doPublish(PubSub srv,  Scanner ent) throws RemoteException {
        System.err.println("Introduzca el nombre del tema");
        if (!ent.hasNextLine()) return false;
        String lin = ent.nextLine();
        Scanner s = new Scanner(lin);
        if  (!s.hasNext()) return false;
        String tname = s.next();
        Map<String,Object> content = Map.of(
                "número de evento", nev++
        );
	Event ev = new Event(tname, content);
        if (srv.publish(ev))
            System.err.println("Se ha publicado el evento: " + ev.toString());
	else 
            System.err.println("Error al publicar el evento: " + ev.toString());
        return true;
    }
    static private boolean doConsumeEvent(PubSub srv,  Scanner ent) throws RemoteException {
        System.err.println("Introduzca el nombre del tema");
        if (!ent.hasNextLine()) return false;
        String lin = ent.nextLine();
        Scanner s = new Scanner(lin);
        if  (!s.hasNext()) return false;
        String tname = s.next();
	Event ev;
	try {
            if ((ev=srv.consumeEvent(tname))!=null)
                System.err.println("Se ha recibido el evento: " + ev.toString());
	    else 
                System.err.println("No ha devuelto evento");
        } catch (RemoteException e) {
            System.err.println("recibida la excepción " + e.getCause());
        }
        return true;
    }
    static private boolean doInitSubscriber(PubSub srv,  Scanner ent) throws RemoteException {
        subscriber = srv.initSubscriber(new Test());
        System.err.println("Se ha iniciado el subscriptor " + subscriber.getUUID());
        return true;
    }
    static private boolean doSubscriberList(PubSub srv,  Scanner ent) throws RemoteException {
        System.out.println("Subscriber List ");
        for (Subscriber s: srv.subscriberList())
            System.out.println("  Subscriber " + s.getUUID());
        return true;
    }
    static private boolean doSubscribeExt(PubSub srv,  Scanner ent, boolean glob) throws RemoteException {
        if (subscriber==null) {
            System.err.println("no se ha dado de alta previamente como subscriptor usando initSubscriber");
            return true;
	}
        System.err.println("Introduzca el nombre del tema");
        if (!ent.hasNextLine()) return false;
        String lin = ent.nextLine();
        Scanner s = new Scanner(lin);
        if  (!s.hasNext()) return false;
        String tname = s.next();
	try {
	    int n = subscriber.subscribe(tname, glob);
            System.err.println("ha devuelto el valor " + n);
        } catch (RemoteException e) {
            System.err.println("recibida la excepción " + e.getCause());
        }
        return true;
    }
    static private boolean doSubscribe(PubSub srv,  Scanner ent) throws RemoteException {
        return doSubscribeExt(srv, ent, false);
    }
    static private boolean doSubscribeGlob(PubSub srv,  Scanner ent) throws RemoteException {
        return doSubscribeExt(srv, ent, true);
    }
    static private boolean doSubscriberListByTopic(PubSub srv,  Scanner ent) throws RemoteException {
        System.err.println("Introduzca el nombre del tema");
        if (!ent.hasNextLine()) return false;
        String lin = ent.nextLine();
        Scanner s = new Scanner(lin);
        if  (!s.hasNext()) return false;
        String tname = s.next();
        Collection<Subscriber> sl = srv.subscriberListByTopic(tname);
	if (sl == null) 
            System.out.println("devuelve un valor nulo");
	else {
            System.out.println("Subscriber List ");
            for (Subscriber su: srv.subscriberListByTopic(tname))
                System.out.println("  Subscriber " + su.getUUID());
        }
        return true;
    }
    static private boolean doGetUUID(PubSub srv,  Scanner ent) throws RemoteException {
        if (subscriber==null) {
            System.err.println("no se ha dado de alta previamente como subscriptor usando initSubscriber");
            return true;
	}
	try {
	    System.err.println("UUID "  + subscriber.getUUID());
        } catch (RemoteException e) {
            System.err.println("recibida la excepción " + e.getCause());
        }
        return true;
    }
    static private boolean doGetEvent(PubSub srv,  Scanner ent) throws RemoteException {
        if (subscriber==null) {
            System.err.println("no se ha dado de alta previamente como subscriptor usando initSubscriber");
            return true;
	}
	try {
	    Event ev = subscriber.getEvent();
            if (ev!=null)
                System.err.println("Se ha recibido el evento: " + ev.toString());
	    else 
                System.err.println("No ha devuelto evento");
        } catch (RemoteException e) {
            System.err.println("recibida la excepción " + e.getCause());
        }
        return true;
    }
    static private boolean doTopicListBySubscriber(PubSub srv,  Scanner ent) throws RemoteException {
        if (subscriber==null) {
            System.err.println("no se ha dado de alta previamente como subscriptor usando initSubscriber");
            return true;
	}
	try {
            System.out.println("Topic List ");
            for (String t: subscriber.topicListBySubscriber())
                System.out.print("  Topic " + t);
            System.out.println("");
        } catch (RemoteException e) {
            System.err.println("recibida la excepción " + e.getCause());
        }
        return true;
    }
    static private boolean doUnsubscribe(PubSub srv,  Scanner ent) throws RemoteException {
        if (subscriber==null) {
            System.err.println("no se ha dado de alta previamente como subscriptor usando initSubscriber");
            return true;
	}
        System.err.println("Introduzca el nombre del tema");
        if (!ent.hasNextLine()) return false;
        String lin = ent.nextLine();
        Scanner s = new Scanner(lin);
        if  (!s.hasNext()) return false;
        String tname = s.next();
	try {
	    boolean n = subscriber.unsubscribe(tname);
            System.err.println("ha devuelto el valor " + n);
        } catch (RemoteException e) {
            System.err.println("recibida la excepción " + e.getCause());
        }
        return true;
    }
    static private boolean doExit(PubSub srv,  Scanner ent) throws RemoteException {
        if (subscriber==null) {
            System.err.println("no se ha dado de alta previamente como subscriptor usando initSubscriber");
            return true;
	}
	try {
	    subscriber.exit();
        } catch (RemoteException e) {
            System.err.println("recibida la excepción " + e.getCause());
        }
        return true;
    }
    static private boolean doDeleteTopic(PubSub srv,  Scanner ent) throws RemoteException {
        System.err.println("Introduzca el nombre del tema");
        if (!ent.hasNextLine()) return false;
        String lin = ent.nextLine();
        Scanner s = new Scanner(lin);
        if  (!s.hasNext()) return false;
        String tname = s.next();
	if (srv.deleteTopic(tname))
            System.err.println("Se ha eliminado el tema");
	else 
            System.err.println("Error al eliminar el tema");
        return true;
    }
    static public void main(String args[]) {
        if (args.length!=2) {
            System.err.println("Usage: Test registryHost registryPort");
            return;
        }
        try {
            PubSub srv = Client.init(args[0], args[1]);
            while (true) {
                boolean formatoOK = false;
                Scanner ent = new Scanner(System.in);
                prompt();
                if (!ent.hasNextLine()) System.exit(0);
                String lin = ent.nextLine();
                Scanner s = new Scanner(lin);
                if (s.hasNext()) {
                   String op = s.next();
                   switch (op) {
                       case "getVersion": formatoOK = doGetVersion(srv, ent); break;
                       case "createTopic": formatoOK = doCreateTopic(srv, ent); break;
                       case "topicList": formatoOK = doTopicList(srv, ent); break;
                       case "publish": formatoOK = doPublish(srv, ent); break;
                       case "getUUID": formatoOK = doGetUUID(srv, ent); break;
                       case "consumeEvent": formatoOK = doConsumeEvent(srv, ent); break;
                       case "initSubscriber": formatoOK = doInitSubscriber(srv, ent); break;
                       case "subscriberList": formatoOK = doSubscriberList(srv, ent); break;
                       case "subscribe": formatoOK = doSubscribe(srv, ent); break;
                       case "subscriberListByTopic": formatoOK = doSubscriberListByTopic(srv, ent); break;
                       case "getEvent": formatoOK = doGetEvent(srv, ent); break;
                       case "subscribeGlob": formatoOK = doSubscribeGlob(srv, ent); break;
                       case "topicListBySubscriber": formatoOK = doTopicListBySubscriber(srv, ent); break;
                       case "unsubscribe": formatoOK = doUnsubscribe(srv, ent); break;
                       case "exit": formatoOK = doExit(srv, ent); break;
                       case "deleteTopic": formatoOK = doDeleteTopic(srv, ent); break;
                    }
                }
                if (!formatoOK)
                     System.err.println("Error en formato de operacion");
            }
        } catch (NotBoundException e) {
            System.err.println("error localizando registry "+ e.toString());
            return;
        } catch (RemoteException e) {
            System.err.println("error de comunicación "+ e.toString());
            return;
        } catch (Exception e) {
            System.err.println("excepción en la ejecución del Test: " + e.toString());
	    e.printStackTrace();
        }
        finally {
            System.exit(0);
        }

    }
}
