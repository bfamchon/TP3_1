package unip.reducers;

import akka.actor.UntypedAbstractActor;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by baptiste on 08/06/17.
 * Hi
 */
public class Reducer extends UntypedAbstractActor{
    private final String name;
    private final Map<String, Integer> words;

    public Reducer(String name) {
        this.name = name;
        this.words = new HashMap<>();
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof String) {
            handleWordReception((String) message);
        }
    }

    private void handleWordReception(String word) {
        System.out.println("Reducer " + this.name + ": handleWordReception(word) " + word);
        if (!this.words.containsKey(word)) {
            this.words.put(word, 1);
        } else {
            this.words.put(word, this.words.get(word)+1);
        }
    }

    @Override
    public void preStart() {
        System.out.println("Reducer " + this.name +": preStart()");

    }
}
