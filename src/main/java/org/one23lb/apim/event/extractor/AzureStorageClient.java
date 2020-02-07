package org.one23lb.apim.event.extractor;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.EnumSet;

import com.microsoft.azure.storage.ResultContinuation;
import com.microsoft.azure.storage.ResultSegment;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.ListBlobItem;

public abstract class AzureStorageClient
{
	private final CloudBlobContainer itsContainer;

	public AzureStorageClient(final AzureStorageClientParameters params)
			throws InvalidKeyException, URISyntaxException, StorageException
	{
		itsContainer = params.createClient();
	}

	public void traverse(final String pattern)
			throws StorageException
	{
		final GlobstarMatcher matcher = new GlobstarMatcher(pattern);

		traverse(matcher, matcher.getBaseDir());
	}

	public abstract void processBlob(final CloudBlob blob)
			throws StorageException;

	private void traverse(final GlobstarMatcher matcher, final String prefix)
			throws StorageException
	{
		ResultContinuation token = null;

		do
		{
			final ResultSegment<ListBlobItem> result = itsContainer.listBlobsSegmented(
					prefix, false, EnumSet.of(BlobListingDetails.METADATA), null, token, null, null);

			for (final ListBlobItem blobItem : result.getResults())
			{
				if (blobItem instanceof CloudBlobDirectory)
				{
					// Note : with flat listing, we should never get here.
					final CloudBlobDirectory dir = (CloudBlobDirectory) blobItem;
					final String dirPath = dir.getPrefix();

					if (matcher.matchesFullPath(dirPath))
					{
						traverse(matcher, dirPath);
					}
				}
				else
				{
					final CloudBlob blob = (CloudBlob) blobItem;
					final String blobName = blob.getName();

					if (matcher.matchesFullPath(blobName))
					{
						if (CmdLine.isDryRun())
							System.out.println(blobName);
						else
							processBlob(blob);
					}
				}
			}

			token = result.getContinuationToken();
		}
		while (token != null);
	}
}
