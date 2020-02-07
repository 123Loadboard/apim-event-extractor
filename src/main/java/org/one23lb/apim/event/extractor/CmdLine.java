package org.one23lb.apim.event.extractor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public final class CmdLine
{
	private static final Options OPTIONS = new Options();
	private static final Option OPT_DRYRUN;
	private static final Option OPT_EVENTHUB;

	static
	{
		OPTIONS.addOption(OPT_DRYRUN = Option.builder()
			.longOpt("dry-run")
			.build()
		);

		OPTIONS.addOption(OPT_EVENTHUB = Option.builder()
			.longOpt("eventhub")
			.build()
		);
	}

	private static boolean itIsDryRun;
	private static boolean itIsEventHubData;
	private static List<String> itsArgs;

	public static boolean isDryRun()
	{
		return itIsDryRun;
	}

	public static boolean isEventHubData()
	{
		return itIsEventHubData;
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

		itIsDryRun = cmd.hasOption(OPT_DRYRUN.getLongOpt());
		itIsEventHubData = cmd.hasOption(OPT_EVENTHUB.getLongOpt());
		itsArgs = Collections.unmodifiableList(new ArrayList<>(cmd.getArgList()));
	}
}
