package com.mongodb.ramp_up_dowm;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lte;
import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;

import org.bson.Document;
import org.bson.types.ObjectId;

public class ConcurrentRepro {
    private static final Random rand = new Random();

    public static class ConcurrentReads {
        static final int CLIENT_COUNT = 10;
        static final int READ_INTERVAL_PER_THREAD_MS = 100;

        private Vector<MongoClient> clients = new Vector<MongoClient>();
        private static final Random rand = new Random();
    
        public ConcurrentReads() throws Exception {
            MongoClientSettings.Builder optionsBuilder = MongoClientSettings.builder();
            ConnectionPoolSettings settings = ConnectionPoolSettings.builder()
                .maxConnectionIdleTime(30000, TimeUnit.MILLISECONDS)
                .maxSize(40)
                .maxWaitTime(60000, TimeUnit.MILLISECONDS)
                .maxConnectionLifeTime(1800000, TimeUnit.MILLISECONDS)
                .build();
            optionsBuilder.applyToConnectionPoolSettings(builder -> builder.applySettings(settings));
    
            ClusterSettings clusterSettings = ClusterSettings.builder()
                .applyConnectionString(new ConnectionString(System.getProperty("mongodb.uri")))
                .localThreshold(50, TimeUnit.MILLISECONDS)
                .build();
            optionsBuilder.applyToClusterSettings(builder -> builder.applySettings(clusterSettings));
            MongoClientSettings clientSettings = optionsBuilder.build();
    
            for (int i = 0; i < CLIENT_COUNT; ++i) {
                try {
                    MongoClient mongoClient = MongoClients.create(clientSettings);
                    // Do a test read.
                    doRead(mongoClient);
                    clients.add(mongoClient);
                } catch (Exception e) {
                    System.out.println("Error creating client: " + e);
                    e.printStackTrace();
                    throw e;
                }
            }
            System.out.println("Initialized clients");
        }

        public ReadThread read() {
            ReadThread t = new ReadThread(this);
            t.start();
            return t;
        }
    
        private MongoClient getClient() {
            int idx = rand.nextInt(clients.size());
            return clients.get(idx);
        }
    
        /**
         * @param mongoClient use null to get a client from the pool.
         * @return first document
         */
        private Document doRead(MongoClient mongoClient) {
            if (mongoClient == null) {
                mongoClient = getClient();
                assert(mongoClient != null);
            }
            try {
                MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
                MongoCollection<Document> gradesCollection = sampleTrainingDB.getCollection("grades");
                long studentId = 10000;
                ArrayList<Document> result = gradesCollection.find(and(
                    gte("student_id", studentId), lte("student_id", studentId + 100))).into(new ArrayList<>());
                assert(!result.isEmpty());
                return result.get(0);
            } catch (Exception e) {
                System.out.println("Error reading: " + e);
                e.printStackTrace();
                throw e;
            }
        }
    }

    static class ReadThread extends Thread {
        protected ConcurrentReads parent;

        public ReadThread(ConcurrentReads parent) {
            this.parent = parent;
        }

        public void run() {
            int loops = 0;
            try {
                for (int i = 0; i < 10; ++i) {
                    Document doc = parent.doRead(null);
                    Thread.sleep(ConcurrentReads.READ_INTERVAL_PER_THREAD_MS);
                    ++loops;
                }
            } catch (Exception e) {
                System.out.println("Read failure: " + e + " on loop " + loops);
                e.printStackTrace();
            }
        }
    }

    private static void addData() {
        try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {

            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
            MongoCollection<Document> gradesCollection = sampleTrainingDB.getCollection("grades");

            List<Document> grades = new ArrayList<>();
            for (long studentId = 10000; studentId <= 10000 + 1000; ++studentId) {
                grades.add(generateNewGrade(studentId, 1d));
            }
            gradesCollection.insertMany(grades, new InsertManyOptions().ordered(false));
            System.out.println("Inserted students");
    
            // Enable sharding.
            Document result = 
                mongoClient.getDatabase("admin").runCommand(new BasicDBObject("enablesharding", "sample_training")); 
            assert(result.get("OK").toString() == "1");

            // Shard collection.
            result = mongoClient.getDatabase("admin").runCommand(
                new BasicDBObject("shardCollection", "sample_training.grades").
                append("key", new BasicDBObject("_id", 1)));
            assert(result.get("OK").toString() == "1");
        }
    }

    private static Document generateNewGrade(double studentId, double classId) {
        List<Document> scores = asList(new Document("type", "exam").append("score", rand.nextDouble() * 100),
                                       new Document("type", "quiz").append("score", rand.nextDouble() * 100),
                                       new Document("type", "homework").append("score", rand.nextDouble() * 100),
                                       new Document("type", "homework").append("score", rand.nextDouble() * 100));
        return new Document("_id", new ObjectId()).append("student_id", studentId)
                                                  .append("class_id", classId)
                                                  .append("scores", scores);
    }

    public static void main(String[] args) throws Exception {
        addData();
        ConcurrentReads concurrentReads = new ConcurrentReads();
        while (true) {
            ReadThread thread = concurrentReads.read();
            Thread.sleep(40);
            thread.join();
        }
    }
}

