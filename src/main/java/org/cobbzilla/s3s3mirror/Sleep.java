package org.cobbzilla.s3s3mirror;

public class Sleep {

    public static boolean sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            System.err.println("interrupted!");
            return true;
        }
        return false;
    }

}
