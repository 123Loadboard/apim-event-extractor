package org.one23lb.apim.event.extractor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public final class CmdLine
{
	private static final Options OPTIONS = new Options();
	private static final Option OPT_DRYRUN;
	private static final Option OPT_EVENTHUB;
	private static final Option OPT_LOCAL;
	private static final Option OPT_DOWNLOAD;
	private static final Option OPT_HELP;

	static
	{
		OPTIONS.addOption(OPT_DRYRUN = Option.builder()
			.desc("Iterate through the directories and display which files are matching, but do nothing with these "
					+ "files.")
			.longOpt("dry-run")
			.build()
		);

		OPTIONS.addOption(OPT_DOWNLOAD = Option.builder("d")
			.longOpt("download")
			.desc("All files matching in Azure Storage will be downloaded to the specificied target directory.")
			.hasArg()
			.build()
		);

		OPTIONS.addOption(OPT_EVENTHUB = Option.builder()
			.desc("All files matching must be Avro files containing EventData structures from Azure EventHub. " +
					"When unspecified, all files matching are assumed to be Avro files containing correlated " +
					"HTTP requests & replies generated by our Azure Databricks pipeline. The content of these " +
					"Avro files is converted to JSON on stdout. This option is ignored when " +
					OPT_DOWNLOAD.getLongOpt() + " is specified. The JSON output of this option can be piped " +
					"into mongoimport (e.g. | mongoimport --host <host> --mode upsert --db=<db> --collection=<coll>)")
			.longOpt("eventhub")
			.build()
		);

		OPTIONS.addOption(OPT_LOCAL = Option.builder("l")
			.longOpt("local")
			.desc("Read files from the local file system instead of Azure Storage.")
			.build()
		);

		OPTIONS.addOption(OPT_HELP = Option.builder("h")
			.longOpt("help")
			.desc("Print usage.")
			.build()
		);
	}

	private static boolean itIsDryRun;
	private static boolean itIsEventHubData;
	private static boolean itIsLocal;
	private static String itsDownloadDir;
	private static List<String> itsArgs;

	public static boolean isDryRun()
	{
		return itIsDryRun;
	}

	public static boolean isEventHubData()
	{
		return itIsEventHubData;
	}

	public static boolean isLocal()
	{
		return itIsLocal;
	}

	public static String getDownload()
	{
		return itsDownloadDir;
	}

	public static List<String> getArgs()
	{
		return itsArgs;
	}

	public static void load(final String[] args)
			throws ParseException
	{
		final CommandLineParser parser = new DefaultParser();
		final CommandLine cmd = parser.parse(OPTIONS, args);

		itsArgs = Collections.unmodifiableList(new ArrayList<>(cmd.getArgList()));

		if (cmd.hasOption(OPT_HELP.getLongOpt()) || itsArgs.isEmpty())
		{
			printUsage();
			System.exit(0);
		}

		itIsDryRun = cmd.hasOption(OPT_DRYRUN.getLongOpt());
		itIsEventHubData = cmd.hasOption(OPT_EVENTHUB.getLongOpt());
		itIsLocal = cmd.hasOption(OPT_LOCAL.getLongOpt());
		itsDownloadDir = cmd.getOptionValue(OPT_DOWNLOAD.getLongOpt());
	}

	public static void safeLoad(final String[] args)
	{
		try
		{
			load(args);
		}
		catch (final ParseException e)
		{
			printUsage();
			System.exit(1);
		}
	}

	private static void printUsage()
	{
		final HelpFormatter formatter = new HelpFormatter();
		final String NEWLINE = formatter.getNewLine();
		final int width = 120;

		formatter.setWidth(width);

		final StringWriter sw = new StringWriter();
		try (final PrintWriter pw = new PrintWriter(sw))
		{
			formatter.printUsage(pw, width, "-jar apim-event-extrator.jar ", OPTIONS);

			pw.flush();
		}

		try (final PrintWriter err = new PrintWriter(System.err))
		{
			formatter.printHelp(
					err,
					formatter.getWidth(),
					sw.toString().trim() + " GLOB#1 GLOB#2 ...",
					"This tool allows interacting with Azure Storage to retrieve and/or process data that was persisted " +
					"by EventHub or Databricks." + NEWLINE +
					"The GLOB patterns support the globstar syntax (https://en.wikipedia.org/wiki/Glob_(programming)#Unix-like).",
					OPTIONS,
					formatter.getLeftPadding(),
					formatter.getDescPadding(),
					"",
					false);

			err.flush();
		}
	}
}
