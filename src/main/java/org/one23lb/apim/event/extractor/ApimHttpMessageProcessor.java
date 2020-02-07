package org.one23lb.apim.event.extractor;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import org.apache.avro.generic.GenericRecord;
import org.one23lb.apim.event.core.EventParser;

import com.google.gson.JsonObject;
import com.microsoft.azure.storage.StorageException;

public class ApimHttpMessageProcessor extends AzureStorageAvroClient<GenericRecord>
{
	public ApimHttpMessageProcessor(final AzureStorageClientParameters params)
			throws InvalidKeyException, URISyntaxException, StorageException
	{
		super(params);
	}

	@Override
	public void processRecord(final GenericRecord record)
	{
		final JsonObject root = new JsonObject();

		root.addProperty("_id", getStringProperty(record, "msgId"));
        root.addProperty("flowId", getStringProperty(record, "flowId"));
        root.addProperty("parentId", getStringProperty(record, "parentId"));
        root.addProperty("opId", getStringProperty(record, "opId"));

        EventParser.validate(root, "requestMetadata", getStringProperty(record, "req_metadata"));
        EventParser.validate(root, "requestBody", getStringProperty(record, "req_body"));
        EventParser.validate(root, "responseMetadata", getStringProperty(record, "rep_metadata"));
        EventParser.validate(root, "responseBody", getStringProperty(record, "rep_body"));

        System.out.println(root.toString());
	}
}
