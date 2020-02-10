package org.one23lb.apim.event.extractor;

import com.microsoft.azure.storage.StorageException;

public abstract class StorageClient
{
	public abstract void traverse(final String pattern)
			throws StorageException;
}
