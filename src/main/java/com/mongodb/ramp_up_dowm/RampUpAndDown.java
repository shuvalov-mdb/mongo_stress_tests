package com.mongodb.ramp_up_dowm;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Updates.set;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.mongodb.client.FindIterable;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;

import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * The test executes in three phases:
 * 1. Ramp up by adding threads until performance saturates
 * 2. Stress tests by keeping thread count unchanged
 * 3. Ramp down by stopping threads.
 */
public class RampUpAndDown {
    static final int MIN_READ_THREADS = 500;
    static final int MAX_READ_THREADS = 32000;
    static final int MIN_WRITE_THREADS = 100;
    static final int MAX_WRITE_THREADS = 10000;

    static final int READ_INTERVAL_PER_THREAD_MS = 100;

    static final int CLIENT_COUNT = 50;

    private final Stats stats;
    private static final Random rand = new Random();

    private final ReentrantLock lock = new ReentrantLock();
    private Vector<MongoClient> clients = new Vector<MongoClient>();
    private ConcurrentLinkedDeque<ReadThread> readThreads = new ConcurrentLinkedDeque<ReadThread>();
    private ConcurrentLinkedDeque<WriteThread> writeThreads = new ConcurrentLinkedDeque<WriteThread>();
    private ConnectionTarget connectionTarget;

    public RampUpAndDown(Stats stats) throws Exception {
        this.stats = stats;
        connectionTarget = new ConnectionTarget(stats, MIN_READ_THREADS + MIN_WRITE_THREADS);
        for (int i = 0; i < CLIENT_COUNT; ++i) {
            try {
                MongoClient mongoClient = createClient();
                clients.add(mongoClient);
            } catch (Exception e) {
                System.out.println("Error creating client: " + e);
                e.printStackTrace();
                throw e;
            }
            if ((i % 100) == 0) {
                System.out.println("Initialized " + i + " clients");
            }
        }
        System.out.println("Initialized clients");
    }

    protected static MongoClient createClient() throws Exception {
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
            .serverSelectionTimeout(30000, TimeUnit.MILLISECONDS)
            .build();
        optionsBuilder.applyToClusterSettings(builder -> builder.applySettings(clusterSettings));
        MongoClientSettings clientSettings = optionsBuilder.build();

        while (true) {
            try {
                MongoClient mongoClient = MongoClients.create(clientSettings);
                // Do a test read.
                MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
                MongoCollection<Document> gradesCollection = sampleTrainingDB.getCollection("grades");
                long studentId = 10000;
                ArrayList<Document> result = gradesCollection.find(
                    eq("student_id", studentId)).into(new ArrayList<>());
                if (result.isEmpty()) {
                    throw new Exception("Can't read");
                }
                return mongoClient;
            } catch (Exception e) {
                System.out.println("Error creating client: " + e);
            }
        }
    }

    void initThreads() {
        lock.lock();
        try {
            while (readThreads.size() < MIN_READ_THREADS) {
                maybeAddReadThreadLocked();
            }
            while (writeThreads.size() < MIN_WRITE_THREADS) {
                maybeAddWriteThreadLocked();
            }
        } finally {
            lock.unlock();
        }
    }

    public Document read() throws InterruptedException, IOException {
        // Find a thread with result.
        while (true) {
            lock.lock();
            try {
                while (readThreads.size() + writeThreads.size() < connectionTarget.getThreadCountTarget() - 1) {
                    maybeAddReadThreadLocked();
                    stats.logStatsIfNeeded();
                    Thread.sleep(1);
                }
                Iterator<ReadThread> it = readThreads.iterator();
                while (it.hasNext()) {
                    ReadThread t = it.next();
                    Thread.State state = t.getState();
                    if (state == Thread.State.TERMINATED) {
                        it.remove();
                        stats.setThreadCount(readThreads.size(), true);
                    }
                    Document doc = t.getResult();
                    if (doc != null) {
                        t.terminate();
                        return doc;
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
            Thread.sleep(10);
        }
    }

    public void write() {
        lock.lock();
        try {
            maybeAddWriteThreadLocked();
        } finally {
            lock.unlock();
        }
    }

    private void maybeAddReadThreadLocked() {
        assert(lock.isLocked());
        if (readThreads.size() + writeThreads.size() >= connectionTarget.getThreadCountTarget()) {
            return;
        }
        ReadThread t = new ReadThread(this, stats);
        readThreads.add(t);
        t.start();
        stats.setThreadCount(readThreads.size(), true);
    }

    private void maybeAddWriteThreadLocked() {
        assert(lock.isLocked());
        while (writeThreads.size() < MAX_WRITE_THREADS &&
            readThreads.size() + writeThreads.size() < connectionTarget.getThreadCountTarget()) {
            WriteThread t = new WriteThread(this, stats);
            writeThreads.add(t);
            t.start();
        }
        Iterator<WriteThread> it;
        int i;
        for (i = 0, it = writeThreads.iterator(); it.hasNext() && i < 100; ++i) {
            WriteThread existingThread = it.next();
            if (existingThread == null || existingThread.getState() != Thread.State.TERMINATED) {
                continue;
            }
            it.remove();
        }
        stats.setThreadCount(writeThreads.size(), false);
    }

    static abstract class WorkThread extends Thread {
        protected RampUpAndDown parent;
        private final ReentrantLock threadLock = new ReentrantLock();
        private boolean terminateFlag = false;
        private Document document;
        private Exception failure = null;
        protected Stats stats;

        public void terminate() {
            threadLock.lock();
            try {
                terminateFlag = true;
            } finally {
                threadLock.unlock();
            }
        }

        public Document getResult() {
            threadLock.lock();
            try {
                Document result = document;
                document = null;
                if (failure != null) {
                    return null;
                }
                return result;
            } finally {
                threadLock.unlock();
            }
        }

        protected boolean shouldContinue() {
            threadLock.lock();
            try {
                return !terminateFlag;
            } finally {
                threadLock.unlock();
            }
        }

        protected void setResult(Document doc) {
            threadLock.lock();
            try {
                document = doc;
                if (document != null) {
                    stats.registerEvent("reads");
                }
            } finally {
                threadLock.unlock();
            }
        }

        protected void setFailure(Exception e) {
            threadLock.lock();
            try {
                failure = e;
            } finally {
                threadLock.unlock();
            }
        }
    }

    static class ReadThread extends WorkThread {
        public ReadThread(RampUpAndDown parent, Stats stats) {
            this.parent = parent;
            this.stats = stats;
        }

        public void run() {
            int loops = 0;
            try {
                while (shouldContinue()) {
                    Document doc = parent.doRead(null, 50);
                    setResult(doc);
                    Thread.sleep(READ_INTERVAL_PER_THREAD_MS);
                    ++loops;
                }
                // System.out.println(String.format("Thread %d is terminated after %d loops", 
                //     Thread.currentThread().getId(), loops));
            } catch (Exception e) {
                setResult(null);
                setFailure(e);
                stats.registerEvent("readFailure");
                //System.out.println("Read failure: " + e + " on loop " + loops);
                e.printStackTrace();
            }
        }
    }

    static class WriteThread extends WorkThread {
        private static final Random rand = new Random();
        public WriteThread(RampUpAndDown parent, Stats stats) {
            this.parent = parent;
            this.stats = stats;
        }

        public void run() {
            int loops = 0;
            MongoClient mongoClient = null;
            try {
                mongoClient = parent.getClient();
                MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
                MongoCollection<Document> gradesCollection = sampleTrainingDB.getCollection("grades")
                    .withWriteConcern(sampleTrainingDB.getWriteConcern().withWTimeout(30000, TimeUnit.MILLISECONDS));

                for (loops = 0; loops < 100; ++loops) {
                    Bson filter = eq("student_id", 10000 + rand.nextInt(1000));
                    Bson updateOperation = set("class_id", rand.nextInt(10));
                    UpdateResult updateResult = gradesCollection.updateOne(filter, updateOperation);
                    if (updateResult.getModifiedCount() > 0) {
                        stats.registerEvent("writes");
                    }
                    Thread.sleep(100);
                }
            } catch (IllegalStateException e) {
                System.out.println("Replace client failed with: " + e);
                parent.replaceClient(mongoClient);
            } catch (Exception e) {
                setResult(null);
                setFailure(e);
                stats.registerEvent("writeFailure");
                System.out.println("Write failure: " + e);
                System.out.println(e);
                e.printStackTrace();
            }
        }
    }

    private MongoClient getClient() {
        lock.lock();
        try {
            return clients.get(rand.nextInt(clients.size()));
        } finally {
            lock.unlock();
        }
    }

    private void replaceClient(MongoClient client) {
        stats.registerEvent("replaceClient");
        lock.lock();
        try {
            for (int i = 0; i < clients.size(); ++i) {
                if (clients.get(i) == client) {
                    try {
                        clients.set(i, createClient());
                        stats.registerEvent("newConnection");
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * @param mongoClient use null to get a client from the pool.
     * @return first document
     */
    private Document doRead(MongoClient mongoClient, int count) throws Exception {
        if (mongoClient == null) {
            mongoClient = getClient();
            assert(mongoClient != null);
        }
        if (count <= 1) {
            count = 100;
        }
        try {
            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
            MongoCollection<Document> gradesCollection = sampleTrainingDB.getCollection("grades");
            long studentId = 10000 + rand.nextInt(50000);
            FindIterable<Document> iterable =
                gradesCollection.find(
                    and(gte("student_id", studentId), lte("student_id", studentId + count)))
                .maxTime(400, TimeUnit.MILLISECONDS);
            Document result = null;
            for (Document doc : iterable) {
                result = doc;
            }
            if (result == null) {
                throw new Exception("No results found");
            }
            return result;
        } catch (IllegalStateException e) {
            System.out.println("Replace client failed with: " + e);
            replaceClient(mongoClient);
            return null;
        } catch (Exception e) {
            System.out.println("Error reading: " + e);
            throw e;
        }
    }

    private void stop() throws IOException {
        while (true) {
            Iterator<ReadThread> it = readThreads.iterator();
            while (it.hasNext()) {
                ReadThread t = it.next();
                Thread.State state = t.getState();
                if (state == Thread.State.TERMINATED) {
                    it.remove();
                    stats.setThreadCount(readThreads.size(), true);
                }
                t.terminate();
            }
            Iterator<WriteThread> it2 = writeThreads.iterator();
            while (it2.hasNext()) {
                WriteThread t = it2.next();
                Thread.State state = t.getState();
                if (state == Thread.State.TERMINATED) {
                    it2.remove();
                    stats.setThreadCount(writeThreads.size(), false);
                }
                t.terminate();
            }
            stats.logStatsIfNeeded();
            if (readThreads.size() + writeThreads.size() == 0) {
                break;
            }
        }
        for (MongoClient client : clients) {
            client.close();
        }
    }

    private boolean shouldContinue() {
        return connectionTarget.getThreadCountTarget() > 0;
    }

    public static void main(String[] args) throws Exception {
        String report = "/tmp/stresstest.txt";
        if (args.length > 0) {
            report = args[0];
        }
        System.out.println("Will generate the report in file " + report);
        Stats stats = new Stats(report, new String[]{ "reads", "writes", Stats.READ_THREADS_KEY, Stats.WRITE_THREADS_KEY });
        RampUpAndDown test = new RampUpAndDown(stats);
        test.initThreads();
        while (test.shouldContinue()) {
            test.read();
            Thread.sleep(20);
            test.write();
            stats.logStatsIfNeeded();
            Thread.sleep(20);
        }
        System.out.println("Terminating...");
        test.stop();
        stats.close();
    }
}
