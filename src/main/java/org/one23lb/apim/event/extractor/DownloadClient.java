package org.one23lb.apim.event.extractor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.microsoft.azure.storage.StorageException;

public class DownloadClient extends DataProcessor
{
	private final File itsDestDir;

	public DownloadClient(final String destDir)
	{
		itsDestDir = new File(destDir);
	}

	@Override
	public void process(final InputStream is, final String streamName) throws StorageException
	{
		final File f = new File(itsDestDir, streamName);

		if (!f.getParentFile().mkdirs())
			throw new StorageException("ERR", "Unable to create " + f.getAbsolutePath(), null);

		try (final FileOutputStream fos = new FileOutputStream(f))
		{
			final byte[] buf = new byte[16384];

			while (true)
			{
				final int avail = is.read(buf);

				if (avail < 0)
					return;

				fos.write(buf, 0, avail);
			}
		}
		catch (final IOException e)
		{
			throw StorageException.translateClientException(e);
		}
	}
}
