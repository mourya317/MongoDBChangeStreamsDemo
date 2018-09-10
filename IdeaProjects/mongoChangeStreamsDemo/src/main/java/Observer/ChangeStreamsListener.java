package Observer;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

public interface ChangeStreamsListener {
    Runnable listen(MongoCollection<Document> eventCollection, String topic, int id);
}
