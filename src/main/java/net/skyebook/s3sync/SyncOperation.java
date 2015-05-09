package net.skyebook.s3sync;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.time.DurationFormatUtils;

/**
 *
 * @author Skye Book
 */
public class SyncOperation {

    private ForkJoinPool forkJoinPool;
    private String sourceBucket;
    private String destinationBucket;

    private AmazonS3Client client;

    private List<S3ObjectSummary> allObjects;
    private long totalKeyCount;
    private long sizeOfAllKeys;

    private List<S3ObjectSummary> retryPool;
    private List<String> badKeys;

    private int currentBufferLength = 0;

    private AtomicLong itemsCopied = new AtomicLong();
    private AtomicLong totalBytesSent = new AtomicLong();

    private long startTime;

    private Timer progressTimer;

    public SyncOperation(AWSCredentials credentials, String sourceBucket, String destinationBucket) {
        this.forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        this.sourceBucket = sourceBucket;
        this.destinationBucket = destinationBucket;

        this.client = new AmazonS3Client(credentials);

        this.allObjects = new ArrayList<>();
        this.retryPool = new ArrayList<>();
        this.badKeys = new ArrayList<>();

        this.progressTimer = new Timer();
    }

    public void execute() {
        buildFullObjectList();

        try {
            copy();
        } catch (InterruptedException ex) {
            Logger.getLogger(SyncOperation.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ExecutionException ex) {
            Logger.getLogger(SyncOperation.class.getName()).log(Level.SEVERE, null, ex);
        }

        progressTimer.cancel();
    }

    private void buildFullObjectList() {
        startTime = System.currentTimeMillis();

        System.out.println("Building Object List...");

        ObjectListing listing = client.listObjects(sourceBucket);
        allObjects.addAll(listing.getObjectSummaries());

        updateCurrentLine(allObjects.size() + "\t\tobjects found");

        while (listing.isTruncated()) {
            listing = client.listNextBatchOfObjects(listing);
            allObjects.addAll(listing.getObjectSummaries());
            updateCurrentLine(allObjects.size() + "\t\tobjects found");
        }

        totalKeyCount = allObjects.size();

        // Now that we have all of the objects, get the total size
        for (S3ObjectSummary objectSummary : allObjects) {
            sizeOfAllKeys += objectSummary.getSize();
        }

        long elapsed = System.currentTimeMillis() - startTime;
        updateCurrentLine("");
        System.out.println(allObjects.size() + "\t\tobjects found with a total size of " + humanReadableByteCount(sizeOfAllKeys, true));
        System.out.println("Built full object list in " + DurationFormatUtils.formatDurationHMS(elapsed));
    }

    public void copy() throws InterruptedException, ExecutionException {
        CompletableFuture<Void> f = CompletableFuture.runAsync(() -> {
            allObjects.parallelStream().forEach(objectSummary -> {
                try {
                    S3Object object = client.getObject(objectSummary.getBucketName(), objectSummary.getKey());
                    PutObjectRequest request = new PutObjectRequest(destinationBucket, object.getKey(), object.getObjectContent(), object.getObjectMetadata());
                    request.setCannedAcl(CannedAccessControlList.PublicRead);
                    PutObjectResult putObject = client.putObject(request);
                    totalBytesSent.addAndGet(object.getObjectMetadata().getContentLength());
                    itemsCopied.incrementAndGet();
                } catch (AmazonClientException ex) {
                    retryPool.add(objectSummary);
                } catch (IllegalArgumentException e) {
                    badKeys.add(objectSummary.getKey());
                }
            });
        }, forkJoinPool);

        progressTimer.scheduleAtFixedRate(new ProgressTask(), 0, 2 * 1000);

        f.get();

        System.out.println("");
        System.out.println("BAD KEYS:");
        for (String key : badKeys) {
            System.out.println("\t" + key);
        }
        System.out.println("END BAD KEYS");

        // Run anything that needs to be retried
        if (!retryPool.isEmpty()) {
            allObjects.clear();

            allObjects.addAll(retryPool);
            retryPool.clear();

            copy();
        }
    }

    /*
     *    This is absolute beauty, grabbed from Stack Overflow when looking for
     *    the same function in Apache Commons http://stackoverflow.com/a/3758880
     */
    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) {
            return bytes + " B";
        }
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    private synchronized void updateCurrentLine(String newContent) {
        System.out.println(newContent);
        
        
        // Write new content
//        System.out.print(newContent);
//
//        if (newContent.length() < currentBufferLength) {
//            for (int i = 0; i < currentBufferLength - newContent.length(); i++) {
//                System.out.print(" ");
//            }
//        }
//
//        System.out.print("\r");
//
//        currentBufferLength = newContent.length();
    }

    private class ProgressTask extends TimerTask {

        private final int numberOfDashes = 20;

        @Override
        public void run() {

            StringBuilder progress = new StringBuilder();

            long soFar = itemsCopied.get();

            float percentage = (float) soFar / (float) totalKeyCount;

            StringBuilder bar = new StringBuilder();
            int howManyDashes = (int) Math.floor(percentage * numberOfDashes);
            for (int i = 0; i < howManyDashes; i++) {

                if (i == howManyDashes - 1) {
                    bar.append(">");
                } else {
                    bar.append("-");
                }
            }

            progress.append("[");
            progress.append(bar.toString());

            int numberOfSpaces = numberOfDashes - howManyDashes;
            for (int i = 0; i < numberOfSpaces; i++) {
                progress.append(" ");
            }

            progress.append("]");

            progress.append("\t[ ").append(soFar).append("/").append(totalKeyCount).append(" ]");

            //progress.append("\n");
            long elapsed = System.currentTimeMillis() - startTime;
            long fullEstimate = (long) Math.floor(elapsed / percentage);
            long remainingEstimate = fullEstimate - elapsed;

            progress.append("\tElapsed:\t").append(DurationFormatUtils.formatDurationHMS(elapsed));
//            progress.append("\n");
            progress.append("\tETA:\t").append(DurationFormatUtils.formatDurationHMS(remainingEstimate));

            updateCurrentLine(progress.toString());
        }

    }
}
