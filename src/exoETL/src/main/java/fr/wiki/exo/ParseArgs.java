package fr.wiki.exo;

import org.apache.commons.cli.*;

public class ParseArgs {

  private static void addOption(Options allOptions, Option optionToAdd, boolean optionNotRequired) {
    optionToAdd.setRequired(optionNotRequired);
    allOptions.addOption(optionToAdd);
  }

  protected static String[] parseArgs(String[] args) {

    Options options = new Options();
    Option date_option = new Option("dateHour", "dateHour", false, "date and hour");
    date_option.setArgs(2);
    addOption(options, date_option, false);

    CommandLineParser parser = new BasicParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (org.apache.commons.cli.ParseException e) {
      formatter.printHelp("fr.wiki.exo", options);
      System.exit(1);
      return args;
    }

    String[] dateHour = cmd.getOptionValues("dateHour");

    return dateHour;

  }

}
