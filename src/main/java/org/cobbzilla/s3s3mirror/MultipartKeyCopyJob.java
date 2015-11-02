package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import java.util.ArrayList;
import java.util.List;

public class MultipartKeyCopyJob extends KeyCopyJob {

    public MultipartKeyCopyJob(AmazonS3Client client, MirrorContext context, S3ObjectSummary summary, Object notifyLock) {
        super(client, context, summary, notifyLock);
    }

    @Override
    boolean keyCopied(ObjectMetadata sourceMetadata, AccessControlList objectAcl) {
        long objectSize = summary.getSize();
        MirrorOptions options = context.getOptions();
        String sourceBucketName = options.getSourceBucket();
        int maxPartRetries = options.getMaxRetries();
        String targetBucketName = options.getDestinationBucket();
        List<CopyPartResult> copyResponses = new ArrayList<CopyPartResult>();
        if (options.isVerbose()) {
            System.out.println("Initiating multipart upload request for " + summary.getKey());
        }
        InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(targetBucketName, keydest)
                .withObjectMetadata(sourceMetadata);

        if (options.isCrossAccountCopy()) {
            initiateRequest.withCannedACL(CannedAccessControlList.BucketOwnerFullControl);
        } else {
            initiateRequest.withAccessControlList(objectAcl);
        }

        InitiateMultipartUploadResult initResult = client.initiateMultipartUpload(initiateRequest);

        long partSize = options.getUploadPartSize();
        long bytePosition = 0;

        for (int i = 1; bytePosition < objectSize; i++) {
            long lastByte = bytePosition + partSize - 1 >= objectSize ? objectSize - 1 : bytePosition + partSize - 1;
            String infoMessage = "copying : " + bytePosition + " to " + lastByte;
            if (options.isVerbose()) {
                System.out.println(infoMessage);
            }
            CopyPartRequest copyRequest = new CopyPartRequest()
                    .withDestinationBucketName(targetBucketName)
                    .withDestinationKey(keydest)
                    .withSourceBucketName(sourceBucketName)
                    .withSourceKey(summary.getKey())
                    .withUploadId(initResult.getUploadId())
                    .withFirstByte(bytePosition)
                    .withLastByte(lastByte)
                    .withPartNumber(i);

            for (int tries = 1; tries <= maxPartRetries; tries++) {
                try {
                    if (options.isVerbose()) System.out.println("try :" + tries);
                    context.getStats().s3copyCount.incrementAndGet();
                    CopyPartResult copyPartResult = client.copyPart(copyRequest);
                    copyResponses.add(copyPartResult);
                    if (options.isVerbose()) System.out.println("completed " + infoMessage);
                    break;
                } catch (Exception e) {
                    if (tries == maxPartRetries) {
                        client.abortMultipartUpload(new AbortMultipartUploadRequest(
                                targetBucketName, keydest, initResult.getUploadId()));
                        e.printStackTrace();
                        System.err.println("Exception while doing multipart copy");
                        return false;
                    }
                }
            }
            bytePosition += partSize;
        }
        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(targetBucketName, keydest,
                initResult.getUploadId(), getETags(copyResponses));
        client.completeMultipartUpload(completeRequest);
        if(options.isVerbose()) {
            System.out.println("completed multipart request for : " + summary.getKey());
        }
        context.getStats().bytesCopied.addAndGet(objectSize);
        return true;
    }

    private List<PartETag> getETags(List<CopyPartResult> copyResponses) {
        List<PartETag> eTags = new ArrayList<PartETag>();
        for (CopyPartResult response : copyResponses) {
            eTags.add(new PartETag(response.getPartNumber(), response.getETag()));
        }
        return eTags;
    }

    @Override
    boolean objectChanged(ObjectMetadata metadata) {
        return summary.getSize() != metadata.getContentLength();
    }
}
