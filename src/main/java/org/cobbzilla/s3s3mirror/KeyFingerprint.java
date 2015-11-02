package org.cobbzilla.s3s3mirror;

public class KeyFingerprint {

    private final long size;
    private final String etag;

    public KeyFingerprint(long size, String etag) {
        this.size = size;
        this.etag = etag;
    }
}
