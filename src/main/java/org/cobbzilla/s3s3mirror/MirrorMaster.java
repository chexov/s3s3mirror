package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;

import java.util.concurrent.*;

/**
 * Manages the Starts a KeyLister and sends batches of keys to the ExecutorService for handling by KeyJobs
 */
public class MirrorMaster {

    public static final String VERSION = System.getProperty("s3s3mirror.version");

    private final AmazonS3Client client;
    private final MirrorContext context;

    public MirrorMaster(AmazonS3Client client, MirrorContext context) {
        this.client = client;
        this.context = context;
    }

    public void mirror() {

        System.out.println("version "+VERSION+" starting");

        final MirrorOptions options = context.getOptions();

        if (options.isVerbose() && options.hasCtime()) System.out.println("will not copy anything older than "+options.getCtime()+" (cutoff="+options.getMaxAgeDate()+")");

        final int maxQueueCapacity = getMaxQueueCapacity(options);
        final BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(maxQueueCapacity);
        final RejectedExecutionHandler rejectedExecutionHandler = new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                System.err.println("Error submitting job: "+r+", possible queue overflow");
            }
        };

        final ThreadPoolExecutor executorService = new ThreadPoolExecutor(options.getMaxThreads(), options.getMaxThreads(), 1, TimeUnit.MINUTES, workQueue, rejectedExecutionHandler);

        final KeyMaster copyMaster = new CopyMaster(client, context, workQueue, executorService);
        KeyMaster deleteMaster = null;

        try {
            copyMaster.start();

            if (context.getOptions().isDeleteRemoved()) {
                deleteMaster = new DeleteMaster(client, context, workQueue, executorService);
                deleteMaster.start();
            }

            while (true) {
                if (copyMaster.isDone() && (deleteMaster == null || deleteMaster.isDone())) {
                    System.out.println("mirror: completed");
                    break;
                }
                if (Sleep.sleep(100)) return;
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Unexpected exception in mirror: "+e);

        } finally {
            try { copyMaster.stop();   } catch (Exception e) { e.printStackTrace();
                System.err.println("Error stopping copyMaster: "+e); }
            if (deleteMaster != null) {
                try { deleteMaster.stop(); } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println("Error stopping deleteMaster: "+e); }
            }
        }
    }

    public static int getMaxQueueCapacity(MirrorOptions options) {
        return 10 * options.getMaxThreads();
    }

}
