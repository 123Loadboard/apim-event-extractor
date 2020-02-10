package org.one23lb.apim.event.extractor;

import java.io.InputStream;

import com.microsoft.azure.storage.StorageException;

public abstract class DataProcessor
{
	public abstract void process(final InputStream is, final String streamName) throws StorageException;
}
