package Observer;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.bson.Document;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static java.util.Arrays.asList;

public class ResumableListener implements ChangeStreamsListener {

    static int count = 0;

    public Runnable listen(final MongoCollection<Document> eventCollection, final String topic, final int id) {
        final MongoCursor<ChangeStreamDocument<Document>> changes1 = eventCollection.watch(asList(
                Aggregates.match( and( asList(
                        in("operationType", asList("insert"))
                        ,
                        eq("fullDocument.topic", topic)))
                ))).iterator();

        Runnable task1=new Runnable() {
            public void run() {
                System.out.println("\t[Listener-"+id+"]Waiting for events on topic ::"+topic);
                //BsonDocument resumeToken = null;
                try {
                    resumeStream(changes1, eventCollection,topic,id);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                    /*while (changes1.hasNext()) {
                        ChangeStreamDocument<Document> change = changes1.next();
                        System.out.println( "event received on topic t2:"+change );

                        resumeToken = change.getResumeToken();
                        if (change.getFullDocument().get("forceResume",Boolean.FALSE)) {
                            System.out.println("\r\nSimulating connection failure for 10 seconds...");
                            try {
                                Thread.sleep(20000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            //changes1.close();
                            break;

                            //resumeStream(newChangeStreamCursor,false);
                        }
                    }

                    MongoCursor<ChangeStreamDocument<Document>> newChangeStreamCursor  = eventCollection.watch(asList(
                            Aggregates.match( and( asList(
                                    in("operationType", asList("insert"))
                                    ,
                                    eq("fullDocument.topic", "t2")))
                            ))).resumeAfter(resumeToken).iterator();
                    System.out.println("\r\nResuming change stream with token " + resumeToken + "\r\n");


                    while (newChangeStreamCursor.hasNext()) {
                        ChangeStreamDocument<Document> change = newChangeStreamCursor.next();
                        System.out.println("event received on topic t2:" + change);
                    }*/
            }
        };
        //new Thread(task1).start();
        return task1;
    }

    private static void resumeStream(MongoCursor<ChangeStreamDocument<Document>> changeStreamCursor, MongoCollection<Document> eventCollection, String topic, int id ) throws InterruptedException {
        BsonDocument resumeToken;

        while (changeStreamCursor.hasNext()) {
            ChangeStreamDocument<Document> change = changeStreamCursor.next();
            //System.out.println("received: " + change.getDocumentKey().toJson() );
            //System.out.println( "[Listener"+id+"]event received on topic:"+topic+"::"+changes.next().getDocumentKey().toJson() );
            System.out.println( "[Listener-"+id+"]event received on topic:"+topic+":Count:"+(++count));
            resumeToken = change.getResumeToken();
            if (change.getFullDocument().get("forceResume",Boolean.FALSE)) {
                System.out.println("\r\nSimulating connection failure for 20 seconds...");
                Thread.sleep(20000);
                //changeStreamCursor.close();
                MongoCursor<ChangeStreamDocument<Document>> newChangeStreamCursor  = eventCollection.watch(asList(
                        Aggregates.match( and( asList(
                                in("operationType", asList("insert"))
                                ,
                                eq("fullDocument.topic", "t2")))
                        ))).resumeAfter(resumeToken).iterator();
                System.out.println("\r\nResuming change stream with token " + resumeToken + "\r\n");
                resumeStream(newChangeStreamCursor, eventCollection, topic,id);
            }
        }
    }
}
