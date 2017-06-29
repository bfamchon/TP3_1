package unip.mappers;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.UntypedAbstractActor;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by baptiste on 08/06/17.
 * Hi
 */
public class Mapper extends UntypedAbstractActor {
    private final String name;
    private List<ActorRef> reducers = new ArrayList<>();


    public Mapper(String name) {
        this.name = name;
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof String) {
            handleLineReception((String) message);
        } else if (message instanceof ActorRef) {
            initReducers((ActorRef) message);
            getSender().tell("reducer init",self());
        }
    }

    private void handleLineReception(String line) {
        System.out.println("Mapper " + this.name + ": handleLineReception()");
        line = line.replaceAll("[^A-Za-z0-9áàâäãåçéèêëíìîïñóòôöõúùûüýÿæœÁÀÂÄÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕÚÙÛÜÝŸÆŒ\\s]", "");
        StringTokenizer st = new StringTokenizer(line, " ");
        String word = st.nextToken();
        while (st.hasMoreTokens()) {
            int index = word.hashCode() % this.reducers.size();
            String url = "akka.tcp://MySystem@localhost:2552/user/red" + index;
            ActorSelection red = getContext().actorSelection(url);
            red.tell(word, self());
            word = st.nextToken();
        }
    }

    private void initReducers(ActorRef red) {
        System.out.println("Mapper " + this.name + ": initReducers()");
        this.reducers.add(red);
    }

    @Override
    public void preStart() {
        System.out.println("Mapper " + this.name + ": preStart()");

    }
}
