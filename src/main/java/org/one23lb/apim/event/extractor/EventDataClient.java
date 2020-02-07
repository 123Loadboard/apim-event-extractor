package org.one23lb.apim.event.extractor;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import org.apache.avro.generic.GenericRecord;
import org.one23lb.apim.event.core.EventParser;

import com.google.gson.JsonObject;
import com.microsoft.azure.storage.StorageException;

public class EventDataClient extends AzureStorageAvroClient<GenericRecord>
{
	public EventDataClient(final AzureStorageClientParameters params)
			throws InvalidKeyException, URISyntaxException, StorageException
	{
		super(params);
	}

	@Override
	public void processRecord(final GenericRecord record) throws StorageException
	{
		final String content = getStringProperty(record, "Body");
		final JsonObject root = EventParser.parse(content);

        System.out.println(root.toString());
	}
}
