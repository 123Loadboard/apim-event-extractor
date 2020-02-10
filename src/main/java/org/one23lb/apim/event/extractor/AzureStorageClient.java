package org.one23lb.apim.event.extractor;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.EnumSet;

import com.microsoft.azure.storage.ResultContinuation;
import com.microsoft.azure.storage.ResultSegment;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobInputStream;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.ListBlobItem;

public class AzureStorageClient extends StorageClient
{
	private final CloudBlobContainer itsContainer;
	private final DataProcessor itsProc;

	public AzureStorageClient(final AzureStorageClientParameters params, final DataProcessor proc)
			throws InvalidKeyException, URISyntaxException, StorageException
	{
		itsContainer = params.createClient();
		itsProc = proc;
	}

	@Override
	public void traverse(final String pattern)
			throws StorageException
	{
		final GlobstarMatcher matcher = new GlobstarMatcher(pattern);

		traverse(matcher, matcher.getBaseDir());
	}

	private void traverse(final GlobstarMatcher matcher, final String prefix)
			throws StorageException
	{
		final EnumSet<BlobListingDetails> listingDetails = null; // EnumSet.of(BlobListingDetails.METADATA)
		ResultContinuation token = null;

		do
		{
			final ResultSegment<ListBlobItem> result = itsContainer.listBlobsSegmented(
					prefix, false, listingDetails, null, token, null, null);

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
						{
							final long length = blob.getProperties().getLength();

							System.out.println(Long.toString(length) + ' ' + blobName);
						}
						else
						{
							processBlob(blob);
						}
					}
				}
			}

			token = result.getContinuationToken();
		}
		while (token != null);
	}

	private void processBlob(final CloudBlob blob)
			throws StorageException
	{
		try (final BlobInputStream blobIS = blob.openInputStream())
		{
			System.out.println("// " + blob.getName());

			itsProc.process(blobIS, blob.getName());
		}
		catch (final StorageException e)
		{
			throw e;
		}
		catch (final Exception e)
		{
			throw StorageException.translateClientException(e);
		}
	}
}
