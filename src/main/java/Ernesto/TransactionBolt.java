package main.java.Ernesto;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import storm.contrib.mongo.MongoBolt;

import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: Michael
 * Date: 8/9/12
 * Time: 11:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class TransactionBolt extends MongoBolt {
    private final String mongoCollectionName;

    public TransactionBolt(
            String mongoHost, int mongoPort, String mongoDbName, String mongoCollectionName) {

        super(mongoHost, mongoPort, mongoDbName);
        this.mongoCollectionName = mongoCollectionName;
    }

    @Override
    public boolean shouldActOnInput(Tuple input) {
        return true;
    }

    @Override
    public String getMongoCollectionForInput(Tuple input) {
        return mongoCollectionName;
    }

    @Override
    public DBObject getDBObjectForInput(Tuple input) {
        /*BasicDBObjectBuilder dbObjectBuilder = new BasicDBObjectBuilder();

        for (String field : input.getFields()) {
            Object value = input.getValueByField(field);
            if (isValidDBObjectField(value)) {
                dbObjectBuilder.append(field, value);
            }
        }

        //return dbObjectBuilder.get();
        */
        String value = input.getValue(0).toString();
        DBObject dbObject = (DBObject)JSON.parse(value);

        return dbObject;
    }

    private boolean isValidDBObjectField(Object value) {
        return value instanceof String
                || value instanceof Date
                || value instanceof Integer
                || value instanceof Float
                || value instanceof Double
                || value instanceof Short
                || value instanceof Long
                || value instanceof DBObject;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { }
}
