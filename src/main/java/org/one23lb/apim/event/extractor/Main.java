package org.one23lb.apim.event.extractor;

import java.util.Properties;

import org.one23lb.apim.event.core.Configuration;

/**
 * https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-java-get-started-send
 *
 */
public class Main
{
	public static void main(final String[] args)
			throws Exception
	{
		try
		{
			CmdLine.load(args);

			final Properties props = Configuration.get();
			final String connectionString = props.getProperty("storage.connectionString");
			final String containerName = props.getProperty("storage.containerName");
			final AzureStorageClientParameters params = new AzureStorageClientParameters(connectionString, containerName);
			final AzureStorageClient client = CmdLine.isEventHubData() ?
					new EventDataClient(params) : new ApimHttpMessageProcessor(params);

			for (final String filename : CmdLine.getArgs())
			{
				client.traverse(filename);
			}
		}
		catch (final Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
}
