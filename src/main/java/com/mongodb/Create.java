// Run with:
//   mvn compile exec:java -Dexec.mainClass="com.mongodb.quickstart.Create" -Dmongodb.uri="mongodb://localhost:27017/test?w=majority"

package com.mongodb;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.InsertManyOptions;

import org.bson.Document;
import org.bson.types.ObjectId;

public class Create {

    private static final Random rand = new Random();

    public static void main(String[] args) {
        try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {

            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
            MongoCollection<Document> gradesCollection = sampleTrainingDB.getCollection("grades");

            insertManyDocuments(gradesCollection);

            // Create shard index.
            gradesCollection.createIndex(Indexes.ascending("student_id"));
            // Enable sharding.
            Document result = 
                mongoClient.getDatabase("admin").runCommand(new BasicDBObject("enablesharding", "sample_training")); 
            assert(result.get("OK").toString() == "1");

            // Shard collection.
            result = mongoClient.getDatabase("admin").runCommand(
                new BasicDBObject("shardCollection", "sample_training.grades").
                append("key", new BasicDBObject("student_id", 1)));
            assert(result.get("OK").toString() == "1");

            for (long id = 10000; id < 1000 * 1000; id += 10000) {
                insertBulkOfStudents(id, id + 10000, gradesCollection);
            }
        }
    }

    private static void insertBulkOfStudents(long studentIdFrom, long studentIdTo, MongoCollection<Document> gradesCollection) {
        List<Document> grades = new ArrayList<>();
        for (long studentId = studentIdFrom; studentId <= studentIdTo; ++studentId) {
            grades.add(generateNewGrade(studentId, 1d));
        }
        gradesCollection.insertMany(grades, new InsertManyOptions().ordered(false));
        System.out.println("Inserted for studentIds " + studentIdFrom + " to " + studentIdTo);
    }

    private static void insertManyDocuments(MongoCollection<Document> gradesCollection) {
        List<Document> grades = new ArrayList<>();
        for (double classId = 1d; classId <= 10d; classId++) {
            grades.add(generateNewGrade(10001d, classId));
        }

        gradesCollection.insertMany(grades, new InsertManyOptions().ordered(false));
        System.out.println("Ten grades inserted for studentId 10001.");
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
}
