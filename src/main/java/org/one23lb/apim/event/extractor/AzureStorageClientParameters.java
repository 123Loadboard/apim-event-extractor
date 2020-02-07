package org.one23lb.apim.event.extractor;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

public class AzureStorageClientParameters
{
	private final String itsConnectionString;
	private final String itsContainerName;

	public AzureStorageClientParameters(final String connectionString, final String containerName)
	{
		itsConnectionString = connectionString;
		itsContainerName = containerName;
	}

	public CloudBlobContainer createClient()
			throws InvalidKeyException, URISyntaxException, StorageException
	{
		final CloudStorageAccount csa = CloudStorageAccount.parse(itsConnectionString);
		final CloudBlobClient cbc = csa.createCloudBlobClient();

		return cbc.getContainerReference(itsContainerName);
	}
}
