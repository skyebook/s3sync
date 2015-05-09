package net.skyebook.s3sync;

import com.amazonaws.auth.BasicAWSCredentials;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;

/**
 *
 * @author Skye Book
 */
public class Sync {

    private static final Logger logger = Logger.getLogger(Sync.class.getName());

    public static void main(String[] args) {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        //checkForHelpOption(args);
        Options options = configureOptions();

        int classpathIndex = -1;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equalsIgnoreCase("-classpath")) {
                classpathIndex = i;
            }
        }

        args = ArrayUtils.subarray(args, classpathIndex + 2, args.length);

        // create the parser
        CommandLineParser parser = new GnuParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("help")) {
                HelpFormatter helpFormatter = new HelpFormatter();
                helpFormatter.printHelp("s3sync", options);
                System.exit(0);
            }

            String sourceBucket = line.getOptionValue("source");
            String destinationBucket = line.getOptionValue("dest");

            BasicAWSCredentials creds = new BasicAWSCredentials(line.getOptionValue("access-key"), line.getOptionValue("secret-key"));

            SyncOperation operation = new SyncOperation(creds, sourceBucket, destinationBucket);

            long start = System.currentTimeMillis();

            operation.execute();

            long elapsed = System.currentTimeMillis() - start;

            System.out.println("Total runtime " + DurationFormatUtils.formatDurationHMS(elapsed));

            System.exit(0);

        } catch (ParseException exp) {
            logger.log(Level.WARNING, "Parsing command-line options failed.  Reason: {0}", exp.getMessage());
            System.exit(1);
        }
    }

    private static void checkForHelpOption(String[] args) {
        Options options = new Options();
        Option help = new Option("help", false, "Print this help message");
        options.addOption(help);

        CommandLineParser parser = new GnuParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args, true);

            if (line.hasOption("help")) {
                HelpFormatter helpFormatter = new HelpFormatter();
                helpFormatter.printHelp("s3sync", configureOptions());
                System.exit(0);
            }
        } catch (ParseException exp) {
            logger.log(Level.WARNING, "Parsing command-line options in help failed.  Reason: {0}", exp.getMessage());
            System.exit(1);
        }
    }

    private static Options configureOptions() {
        Options options = new Options();

        Option help = new Option("h", "help", false, "Print this help message");

        Option source = OptionBuilder.withLongOpt("source").withDescription("Bucket to sync from").hasArg().withArgName("bucket").withType(String.class).isRequired().create();
        Option destination = OptionBuilder.withLongOpt("dest").withDescription("Bucket to sync to").hasArg().withArgName("bucket").withType(String.class).isRequired().create();
        Option accessKey = OptionBuilder.withLongOpt("access-key").withDescription("AWS Access Key").hasArg().withArgName("aws-access-key").withType(String.class).isRequired().create();
        Option secretKey = OptionBuilder.withLongOpt("secret-key").withDescription("AWS Secret Key").hasArg().withArgName("aws-secret-key").withType(String.class).isRequired().create();

        options.addOption(help);
        options.addOption(source);
        options.addOption(destination);
        options.addOption(accessKey);
        options.addOption(secretKey);

        return options;
    }
}
