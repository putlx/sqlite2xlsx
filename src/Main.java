import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.File;

public class Main {
    public static void main(String[] args) {
        final var options = new Options();
        options.addOption(Option.builder("p")
                .longOpt("ptable")
                .hasArgs()
                .argName("TABLE")
                .desc("primary tables, each of them recursively left joins its reference tables")
                .build());
        options.addOption(new Option("h", "help", false, "display this information and exit"));
        var parser = new DefaultParser();
        try {
            var line = parser.parse(options, args);
            var files = line.getArgs();
            if (line.hasOption("h") || files.length == 0) {
                var formatter = new HelpFormatter();
                formatter.printHelp("sqlite2xls database... [options]", options);
            } else if (line.hasOption("p") && files.length > 1) {
                System.err.println("more than one database included while primary tables specified");
            } else {
                for (var input : files) {
                    if (!(new File(input)).isFile()) {
                        System.err.printf("no such file: %s\n", input);
                        continue;
                    }
                    var converter = new Converter();
                    if (line.hasOption("p")) {
                        converter.read(input, line.getOptionValues("p"));
                    } else {
                        converter.read(input);
                    }
                    var output = input;
                    if (output.toLowerCase().endsWith(".db")) {
                        output = output.substring(0, output.length() - 3);
                    }
                    output += ".xls";
                    converter.write(output);
                    System.out.printf("%s -> %s\n", input, output);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
