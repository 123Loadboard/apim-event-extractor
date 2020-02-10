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
			CmdLine.safeLoad(args);

			final String downloadDir = CmdLine.getDownload();

			final DataProcessor proc;

			if (downloadDir != null)
			{
				proc = new DownloadClient(downloadDir);
			}
			else
			{
				proc = CmdLine.isEventHubData() ?
						new EventDataProcessor() : new ApimHttpMessageProcessor();
			}

			final StorageClient client;

			if (CmdLine.isLocal())
			{
				client = new LocalClient(proc);
			}
			else
			{
				final Properties props = Configuration.get();
				final String connectionString = props.getProperty("storage.connectionString");
				final String containerName = props.getProperty("storage.containerName");

				if (connectionString == null)
				{
					System.err.println("you must specify the connection string.");
					System.exit(1);
				}

				if (containerName == null)
				{
					System.err.println("you must specify the container name.");
					System.exit(1);
				}

				final AzureStorageClientParameters params = new AzureStorageClientParameters(
						connectionString, containerName);

				client = new AzureStorageClient(params, proc);
			}

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
