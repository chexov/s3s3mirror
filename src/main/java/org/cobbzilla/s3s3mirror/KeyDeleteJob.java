package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

public class KeyDeleteJob extends KeyJob {

    private String keysrc;

    public KeyDeleteJob (AmazonS3Client client, MirrorContext context, S3ObjectSummary summary, Object notifyLock) {
        super(client, context, summary, notifyLock);

        final MirrorOptions options = context.getOptions();
        keysrc = summary.getKey(); // NOTE: summary.getKey is the key in the destination bucket
        if (options.hasPrefix()) {
            keysrc = keysrc.substring(options.getDestPrefixLength());
            keysrc = options.getPrefix() + keysrc;
        }
    }

    @Override
    public void run() {
        final MirrorOptions options = context.getOptions();
        final MirrorStats stats = context.getStats();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();
        final String key = summary.getKey();
        try {
            if (!shouldDelete()) return;

            final DeleteObjectRequest request = new DeleteObjectRequest(options.getDestinationBucket(), key);

            if (options.isDryRun()) {
                System.out.println("Would have deleted "+key+" from destination because "+keysrc+" does not exist in source");
            } else {
                boolean deletedOK = false;
                for (int tries=0; tries<maxRetries; tries++) {
                    if (verbose) System.out.println("deleting (try #"+tries+"): "+key);
                    try {
                        stats.s3deleteCount.incrementAndGet();
                        client.deleteObject(request);
                        deletedOK = true;
                        if (verbose) System.out.println("successfully deleted (on try #"+tries+"): "+key);
                        break;

                    } catch (AmazonS3Exception s3e) {
                        s3e.printStackTrace();
                        System.err.println("s3 exception deleting (try #"+tries+") "+key+": "+s3e);

                    } catch (Exception e) {
                        e.printStackTrace();
                        System.err.println("unexpected exception deleting (try #"+tries+") "+key+": ");
                    }
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        System.err.println("interrupted while waiting to retry key: "+key);
                        break;
                    }
                }
                if (deletedOK) {
                    context.getStats().objectsDeleted.incrementAndGet();
                } else {
                    context.getStats().deleteErrors.incrementAndGet();
                }
            }

        } catch (Exception e) {
            System.err.println("error deleting key: "+key+": "+e);

        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (verbose) System.out.println("done with "+key);
        }
    }

    private boolean shouldDelete() {

        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();

        // Does it exist in the source bucket
        try {
            ObjectMetadata metadata = getObjectMetadata(options.getSourceBucket(), keysrc, options);
            return false; // object exists in source bucket, don't delete it from destination bucket

        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                if (verbose) System.out.println("Key not found in source bucket (will delete from destination): "+ keysrc);
                return true;
            } else {
                System.out.println("Error getting metadata for " + options.getSourceBucket() + "/" + keysrc + " (not deleting): " + e);
                return false;
            }
        } catch (Exception e) {
            System.out.println("Error getting metadata for " + options.getSourceBucket() + "/" + keysrc + " (not deleting): " + e);
            return false;
        }
    }

}
