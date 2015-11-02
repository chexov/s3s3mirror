package org.cobbzilla.s3s3mirror;

public class MirrorContext {

    MirrorOptions options;
    final MirrorStats stats = new MirrorStats();

    public MirrorContext(MirrorOptions options) {
        this.options = options;
    }

    public MirrorStats getStats() {
        return stats;
    }

    public MirrorOptions getOptions() {
        return options;
    }
}
