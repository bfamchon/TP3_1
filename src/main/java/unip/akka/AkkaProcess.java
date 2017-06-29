package unip.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import akka.actor.Props;
import scala.concurrent.duration.Duration;
import unip.mappers.Mapper;
import unip.master.Master;
import unip.reducers.Reducer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static akka.actor.TypedActor.self;

/**
 * Created by baptiste on 08/06/17.
 * Hi
 */
public class AkkaProcess {

    private ActorSystem system;

    public AkkaProcess() {
        this.system = ActorSystem.create("MySystem");
    }

    public void run() {
        List<ActorRef> mappers = new ArrayList<>();
        List<ActorRef> reducers = new ArrayList<>();

        List<Inbox> inboxList = new ArrayList<>();
        String fileUrl = "textes/zola-assommoir.txt";

        ActorRef master = this.system.actorOf(Props.create(Master.class), "master");

        mappers.add(this.system.actorOf(Props.create(Mapper.class, "map0"), "map0"));
        mappers.add(this.system.actorOf(Props.create(Mapper.class, "map1"), "map1"));
        mappers.add(this.system.actorOf(Props.create(Mapper.class, "map2"), "map2"));

        reducers.add(this.system.actorOf(Props.create(Reducer.class, "red0"), "red0"));
        reducers.add(this.system.actorOf(Props.create(Reducer.class, "red1"), "red1"));

        for (ActorRef map : mappers) {
            throwMappers(map, inboxList, master);
        }
        for (ActorRef red : reducers) {
            throwReducers(red, inboxList, mappers);
        }
        try {
            for (Inbox i : inboxList) {
                i.receive(Duration.create(10, TimeUnit.SECONDS));
            }
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        throwFileUrl(fileUrl, inboxList, master);
    }

    private void throwMappers(ActorRef map, List<Inbox> inboxList, ActorRef master) {
        Inbox inbox = Inbox.create(this.system);
        inboxList.add(inbox);
        inbox.send(master, map);
    }

    private void throwReducers(ActorRef red, List<Inbox> inboxList, List<ActorRef> mappers) {
        for (ActorRef map : mappers) {
            Inbox inbox = Inbox.create(this.system);
            inboxList.add(inbox);
            inbox.send(map, red);
        }
    }

    private void throwFileUrl(String fileUrl, List<Inbox> inboxList, ActorRef master) {
        Inbox inbox = Inbox.create(this.system);
        inboxList.add(inbox);
        inbox.send(master, fileUrl);
    }
}
