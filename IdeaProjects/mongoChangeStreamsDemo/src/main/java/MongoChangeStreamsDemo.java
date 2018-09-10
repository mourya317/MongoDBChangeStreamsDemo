import Observer.Listener;
import Observer.ResumableListener;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.mongodb.client.model.Filters.*;
import static java.util.Arrays.asList;

    public class MongoChangeStreamsDemo  {

        private static ExecutorService executorService;
        static  MongoCollection<Document> eventCollection;
        final static int NO_OF_CONSUMERS = 200;

        public static void main(String[] args) throws Exception {


            init();

            for (int i = 0; i < 150; i++) {

                executorService.execute(new Listener().listen(eventCollection,"t1",i));
            }
            executorService.execute(new ResumableListener().listen(eventCollection,"t2",10));

            System.out.println();
            executorService.shutdown();
            // Wait until all threads are finish
            while (!executorService.isTerminated()) {

            }
            System.out.println("\nFinished all threads");

        }


        public static void init(){
            eventCollection =
                    new MongoClient(
                            new MongoClientURI("mongodb://10.60.98.224:27017,10.60.98.224:27018,10.60.98.224:27019/test?replicaSet=rs1"))
                            .getDatabase("test")
                            .getCollection("testCollection");

            executorService = Executors.newFixedThreadPool(NO_OF_CONSUMERS);
        }


        private static void resumeStream(MongoCursor<ChangeStreamDocument<Document>> changeStreamCursor ) throws InterruptedException {
            BsonDocument resumeToken;
            while (changeStreamCursor.hasNext()) {
                ChangeStreamDocument<Document> change = changeStreamCursor.next();
                System.out.println("received: " + change);
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
                        resumeStream(newChangeStreamCursor);
                    }
                }
            }
        }


