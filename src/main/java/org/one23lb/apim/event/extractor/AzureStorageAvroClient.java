package org.one23lb.apim.event.extractor;

import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;

import org.apache.avro.InvalidAvroMagicException;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobInputStream;
import com.microsoft.azure.storage.blob.CloudBlob;

public abstract class AzureStorageAvroClient<D> extends AzureStorageClient
{
	public AzureStorageAvroClient(final AzureStorageClientParameters params)
			throws InvalidKeyException, URISyntaxException, StorageException
	{
		super(params);
	}

	@Override
	public void processBlob(final CloudBlob blob) throws StorageException
	{
		final DatumReader<D> datumReader = new GenericDatumReader<>();

		try (final BlobInputStream blobIS = blob.openInputStream();
			 final DataFileStream<D> dataFileReader = new DataFileStream<>(blobIS, datumReader))
		{
			System.out.println("// " + blob.getName());
			System.out.println("// " + dataFileReader.getSchema().toString(false));

			while (dataFileReader.hasNext())
			{
				final D record = dataFileReader.next();

				processRecord(record);
			}
		}
		catch (final InvalidAvroMagicException e)
		{
			System.out.println("// Not an avro file : " + blob.getName());
		}
		catch (final Exception e)
		{
			throw StorageException.translateClientException(e);
		}
	}

	public abstract void processRecord(final D record) throws StorageException;

	protected final String getStringProperty(final GenericRecord record, final String propName)
	{
		final Object val = record.get(propName);

		if (val == null)
			return null;

		if (val instanceof ByteBuffer)
			return new String(((ByteBuffer)val).array(), StandardCharsets.UTF_8);

		return val.toString();
	}
}
