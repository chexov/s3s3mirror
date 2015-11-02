package org.cobbzilla.s3s3mirror;

class S3Asset {
    public String bucket;
    public String key;

    public S3Asset(String source, String key) {
        this.bucket = source;
        this.key = key;
    }
}
