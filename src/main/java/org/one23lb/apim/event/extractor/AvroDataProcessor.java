package org.one23lb.apim.event.extractor;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import com.microsoft.azure.storage.StorageException;

public abstract class AvroDataProcessor<D> extends DataProcessor
{
	private static final Logger LOG = Logger.getLogger(AvroDataProcessor.class.getName());

	@Override
	public void process(final InputStream is, String streamName) throws StorageException
	{
		final DatumReader<D> datumReader = new GenericDatumReader<>();

		try (final DataFileStream<D> dataFileReader = new DataFileStream<>(is, datumReader))
		{
			LOG.fine(dataFileReader.getSchema().toString(false));

			while (dataFileReader.hasNext())
			{
				final D record = dataFileReader.next();

				processRecord(record);
			}
		}
		catch (final Exception e)
		{
			throw StorageException.translateClientException(e);
		}
	}

	protected abstract void processRecord(final D record) throws StorageException;

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
