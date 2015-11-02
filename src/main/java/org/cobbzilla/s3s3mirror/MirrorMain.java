package org.cobbzilla.s3s3mirror;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import org.kohsuke.args4j.CmdLineParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class MirrorMain {

    private String[] args;

    private final MirrorOptions options = new MirrorOptions();

    private final CmdLineParser parser = new CmdLineParser(options);

    private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = new Thread.UncaughtExceptionHandler() {
        @Override public void uncaughtException(Thread t, Throwable e) {
            System.err.println("Uncaught Exception (thread "+t.getName()+"): "+e);
        }
    };

    private AmazonS3Client client;
    private MirrorContext context;
    private MirrorMaster master;

    public MirrorMain(String[] args) { this.args = args; }

    public static void main (String[] args) {
        MirrorMain main = new MirrorMain(args);
        main.run();
    }

    public void run() {
        init();
        master.mirror();
    }

    public void init() {
        if (client == null) {
            try {
                parseArguments();
            } catch (Exception e) {
                System.err.println(e.getMessage());
                parser.printUsage(System.err);
                System.exit(1);
            }

            client = getAmazonS3Client();
            context = new MirrorContext(options);
            master = new MirrorMaster(client, context);

            Runtime.getRuntime().addShutdownHook(context.getStats().getShutdownHook());
            Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
    }

    protected AmazonS3Client getAmazonS3Client() {
        ClientConfiguration clientConfiguration = new ClientConfiguration().withProtocol((options.isSsl() ? Protocol.HTTPS : Protocol.HTTP))
                .withMaxConnections(options.getMaxConnections());
        if (options.getHasProxy()) {
            clientConfiguration = clientConfiguration
                    .withProxyHost(options.getProxyHost())
                    .withProxyPort(options.getProxyPort());
        }
        AmazonS3Client client = null;
        if (options.hasAwsKeys()) {
            client = new AmazonS3Client(options, clientConfiguration);
        } else if (options.isUseIamRole()) {
            client = new AmazonS3Client(new InstanceProfileCredentialsProvider(), clientConfiguration);
        } else {
            throw new IllegalStateException("No authenication method available, please specify IAM Role usage or AWS key and secret");
        }        
        if (options.hasEndpoint()) client.setEndpoint(options.getEndpoint());
        return client;
    }

    protected void parseArguments() throws Exception {
        parser.parseArgument(args);
        
        // for credentials, check for IAM role usage if not then...
        // try the .aws/config file first if there is a profile specified, otherwise defer to
        // .s3cfg before using the default .aws/config credentials 
        // (this may attempt .aws/config twice for no reason, but maintains backward compatibility)
        if (options.isUseIamRole() == false) {
            if (!options.hasAwsKeys() && options.getProfile() != null) loadAwsKeysFromAwsConfig();
            if (!options.hasAwsKeys()) loadAwsKeysFromS3Config();
            if (!options.hasAwsKeys()) loadAwsKeysFromAwsConfig();
            if (!options.hasAwsKeys()) {
                throw new IllegalStateException("Could not find credentials, IAM Role usage not specified and ENV vars not defined: " + MirrorOptions.AWS_ACCESS_KEY + " and/or " + MirrorOptions.AWS_SECRET_KEY);
            }
        } else {
            InstanceProfileCredentialsProvider client = new InstanceProfileCredentialsProvider();
            if (client.getCredentials() == null) {
                throw new IllegalStateException("Could not find IAM Instance Profile credentials from the AWS metadata service.");
            }
        }
        options.initDerivedFields();
    }

    private void loadAwsKeysFromS3Config() {
        try {
            // try to load from ~/.s3cfg
            BufferedReader reader = new BufferedReader(new FileReader(System.getProperty("user.home")+File.separator+".s3cfg"));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().startsWith("access_key")) {
                    options.setAWSAccessKeyId(line.substring(line.indexOf("=") + 1).trim());
                } else if (line.trim().startsWith("secret_key")) {
                    options.setAWSSecretKey(line.substring(line.indexOf("=") + 1).trim());
                } else if (!options.getHasProxy() && line.trim().startsWith("proxy_host")) {
                    options.setProxyHost(line.substring(line.indexOf("=") + 1).trim());
                } else if (!options.getHasProxy() && line.trim().startsWith("proxy_port")){
                    options.setProxyPort(Integer.parseInt(line.substring(line.indexOf("=") + 1).trim()));
                }
            }
        } catch (Exception e) {
            // ignore - let other credential-discovery processes have a crack
        }
    }

    private void loadAwsKeysFromAwsConfig() {
        try {
            // try to load from ~/.aws/config
            BufferedReader reader = new BufferedReader(new FileReader(
                    System.getProperty("user.home") + File.separator + ".aws" + File.separator + "config"));
            String line;
            boolean skipSection = true;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("[")) {
                    // if no defined profile, use '[default]' otherwise use profile with matching name
                    if ((options.getProfile() == null && line.equals("[default]"))
                            || (options.getProfile() != null && line.equals("[profile " + options.getProfile() + "]"))) {
                        skipSection = false;
                    } else {
                        skipSection = true;
                    }
                    continue;
                }
                if (skipSection) continue;
                if (line.startsWith("aws_access_key_id")) {
                    options.setAWSAccessKeyId(line.substring(line.indexOf("=") + 1).trim());
                } else if (line.startsWith("aws_secret_access_key")) {
                    options.setAWSSecretKey(line.substring(line.indexOf("=") + 1).trim());
                }
            }
        } catch (Exception e) {
            // ignore - let other credential-discovery processes have a crack
        }
    }

    public AmazonS3Client getClient() {
        return client;
    }

    public MirrorContext getContext() {
        return context;
    }

    public MirrorOptions getOptions() {
        return options;
    }
}
