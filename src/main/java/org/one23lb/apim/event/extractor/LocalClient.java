package org.one23lb.apim.event.extractor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Logger;

import com.microsoft.azure.storage.StorageException;

public class LocalClient extends StorageClient
{
	private static final Logger LOG = Logger.getLogger(LocalClient.class.getName());

	private final DataProcessor itsProc;

	public LocalClient(final DataProcessor proc)
	{
		itsProc = proc;
	}

	@Override
	public void traverse(final String pattern) throws StorageException
	{
		final GlobstarMatcher matcher = new GlobstarMatcher(pattern);
		final String baseDir = matcher.getBaseDir();

		traverse(matcher, baseDir, new File(baseDir));
	}

	private void traverse(final GlobstarMatcher matcher, final String prefix, final File f)
			throws StorageException
	{
		if (f.isFile())
		{
			if (matcher.matchesFullPath(prefix))
			{
				if (CmdLine.isDryRun())
				{
					System.out.println(prefix);
				}
				else
				{
					try (final FileInputStream fis = new FileInputStream(f))
					{
						itsProc.process(fis, prefix);
					}
					catch (final IOException e)
					{
						throw StorageException.translateClientException(e);
					}
				}
			}
		}
		else if (f.isDirectory())
		{
			final String dirPath = prefix.endsWith("/") ? prefix : prefix + '/';

			if (matcher.matchesFullPath(dirPath))
			{
				final File[] files = f.listFiles();

				for (final File file : files)
				{
					traverse(matcher, dirPath, file);
				}
			}
		}
		else
		{
			LOG.warning("Unknown file type : " + prefix);
		}
	}
}
