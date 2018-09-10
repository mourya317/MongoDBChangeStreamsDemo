package Observer;

import com.mongodb.Block;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;

import java.util.Date;

import static com.mongodb.client.model.Filters.*;
import static java.util.Arrays.asList;

public class Listener implements ChangeStreamsListener {

    public Runnable listen(final MongoCollection<Document> eventCollection , final String topic, final int id) {


       /* final MongoCursor<ChangeStreamDocument<Document>> changes = eventCollection.watch(asList(
                Aggregates.match( and( asList(
                        in("operationType", asList("insert"))
                        ,
                        eq("fullDocument.topic", topic)))
                ))).forEach(new Block<ChangeStreamDocument<Document>>(){

            public void apply(ChangeStreamDocument<Document> documentChangeStreamDocument) {

            }
        });*/

        Runnable task=new Runnable() {
            public void run() {
                final int[] count = {0};
                final Double[] lastSecondEventCount = {0.0};
                final Double[] totalDelay = {0.0};
                System.out.println("\t[Listener"+id+"]Waiting for events on topic::"+topic);
                /*while (changes.hasNext()) {
                    //System.out.println( "[Listener"+id+"]event received on topic:"+topic+"::"+changes.next().getDocumentKey().toJson() );
                    System.out.println( "[Listener"+id+"]event received on topic:"+topic+":Count:"+(++count));
                    //do something

                }*/

                new Thread(new Runnable() {
                    public void run() {
                        while(true){
                            try {
                                Thread.sleep(10000);
                                if(lastSecondEventCount[0] > 0) {
                                    System.out.println("Processing Rate Watcher: Number of events received per second on Listener" + id + " on topic:" + topic +": with avg delay:"+(totalDelay[0]/lastSecondEventCount[0])+"::"+lastSecondEventCount[0]/10);
                                    lastSecondEventCount[0] = 0.0;
                                    totalDelay[0] = 0.0;
                                }
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }).start();

                eventCollection.watch(asList(
                        Aggregates.match( and( asList(
                                in("operationType", asList("insert"))
                                ,
                                eq("fullDocument.topic", topic)))
                        ))).forEach(new Block<ChangeStreamDocument<Document>>(){

                    public void apply(ChangeStreamDocument<Document> documentChangeStreamDocument) {
                        //System.out.println("created:"+(Long)documentChangeStreamDocument.getFullDocument().get("created"));
                        //correcting time as per mongo setup
                        Long delay = (new Date().getTime() - (Long)documentChangeStreamDocument.getFullDocument().get("created"));
                        totalDelay[0]+=(delay);
                        lastSecondEventCount[0]++;
                        //System.out.println( "[Listener-"+id+"]event received on topic:"+topic+":Count:"+(++count[0])+":doc:"+documentChangeStreamDocument.getFullDocument().get("created")+":Delay:"+delay+":totalDelay:"+totalDelay[0]);
                    }
                });
            }





        };
        //new Thread(task).start();
        return task;

    }


}
