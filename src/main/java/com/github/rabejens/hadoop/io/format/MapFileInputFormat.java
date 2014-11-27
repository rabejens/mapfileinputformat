package com.github.rabejens.hadoop.io.format;

import static org.apache.commons.io.Charsets.US_ASCII;
import static org.apache.hadoop.io.MapFile.DATA_FILE_NAME;
import static org.apache.hadoop.io.MapFile.INDEX_FILE_NAME;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.binary.Base64InputStream;
import org.apache.commons.codec.binary.Base64OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

/**
 * This is a special {@link FileInputFormat} for {@link MapFile}s. It works very
 * similar to the {@link SequenceFileInputFormat}. The difference is that the
 * input files must be {@link MapFile}s.
 *
 * @param K
 *            Data type of the keys. Must be a sub type of
 *            {@link WritableComparable}.
 * @param V
 *            Data type of the values. Can be any sub type of {@link Writable}.
 *
 * @author Jens Rabe
 */
public class MapFileInputFormat<K extends WritableComparable<K>, V extends Writable>
		extends FileInputFormat<K, V> {

	private static final List<String> MAP_FILE_NAMES = Arrays.asList(
			DATA_FILE_NAME, INDEX_FILE_NAME);

	@Override
	public RecordReader<K, V> createRecordReader(final InputSplit split,
			final TaskAttemptContext context) throws IOException,
			InterruptedException {
		final Configuration conf = context.getConfiguration();
		final K minKey = getMinKey(conf);
		final K maxKey = getMaxKey(conf);
		return new MapFileRecordReader<>(minKey, maxKey);
	}

	@Override
	protected List<FileStatus> listStatus(final JobContext job)
			throws IOException {
		List<FileStatus> superStatus = super.listStatus(job);
		final Configuration conf = job.getConfiguration();
		return getMapFileStatuses(superStatus, conf);
	}

	/* package-private for tests */
	static List<FileStatus> getMapFileStatuses(List<FileStatus> superStatus,
			Configuration conf) throws IOException {
		/* Get all MapFiles from the original list */
		final List<FileStatus> mapFiles = new ArrayList<>();
		for (final FileStatus status : superStatus) {
			/* Check if the file status belongs to a MapFile */
			final Path path = status.getPath();
			final FileSystem fs = path.getFileSystem(conf);
			final Path parent = path.getParent();
			if (!((status.isDirectory() && check(path, fs)) || (status.isFile() && check(
					parent, fs)))) {
				/*
				 * Only consider if the path is a directory and the check
				 * succeeds, or if the path is a file and the check of the
				 * parent directory succeeds
				 */
				continue;
			}
			/* Add the file status of the DATA file */
			final FileStatus toAdd = status.isDirectory() ? fs
					.getFileStatus(new Path(path, DATA_FILE_NAME)) : fs
					.getFileStatus(new Path(parent, DATA_FILE_NAME));
			if (!mapFiles.contains(toAdd)) {
				mapFiles.add(toAdd);
			}
		}
		return mapFiles;
	}

	private static boolean check(final Path path, final FileSystem fs)
			throws FileNotFoundException, IOException {
		final FileStatus[] contents = fs.listStatus(path);
		final int numContents = contents.length;
		if (numContents != 2) {
			/* A MapFile is a directory with exactly two entries */
			return false;
		}
		/* Check if there are both the data and index files */
		return MAP_FILE_NAMES.contains(contents[0].getPath().getName())
				&& MAP_FILE_NAMES.contains(contents[1].getPath().getName());
	}

	/**
	 * Set the minimum key (inclusive) to process
	 *
	 * @param minKey
	 *            The minimum key. Must be {@link WritableComparable}.
	 * @param conf
	 *            The {@link Configuration} to put the key into.
	 * @throws IOException
	 *             if serialization of the key fails.
	 */
	public static <K extends WritableComparable<K>> void setMinKey(
			final K minKey, final Configuration conf) throws IOException {
		setKey("minkey", minKey, conf);
	}

	/**
	 * Set the maximum key (exclusive) to process
	 *
	 * @param maxKey
	 *            The maximum key. Must be {@link WritableComparable}.
	 * @param conf
	 *            The {@link Configuration} to put the key into.
	 * @throws IOException
	 *             if serialization of the key fails.
	 */
	public static <K extends WritableComparable<K>> void setMaxKey(
			final K maxKey, final Configuration conf) throws IOException {
		setKey("maxkey", maxKey, conf);
	}

	private static <K extends WritableComparable<K>> void setKey(
			final String which, final K key, final Configuration conf)
			throws IOException {
		if (key == null) {
			/* For null keys, only unset. */
			conf.unset(which);
			return;
		}
		/*
		 * Serialize the key and add it to the configuration as a Base64 encoded
		 * string.
		 */
		String serialized;
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			try (DataOutputStream dos = new DataOutputStream(
					new Base64OutputStream(baos))) {
				/*
				 * To circumvent Java's type erasure, we must write the class
				 * name too.
				 */
				dos.writeUTF(key.getClass().getName());
				key.write(dos);
			}
			baos.flush();
			serialized = new String(baos.toByteArray(), US_ASCII);
		}
		conf.set(MapFileInputFormat.class.getName() + "." + which, serialized);
	}

	/* package-private for tests */
	static <K extends WritableComparable<K>> K getMinKey(
			final Configuration conf) throws IOException {
		return getKey("minkey", conf);
	}

	/* package-private for tests */
	static <K extends WritableComparable<K>> K getMaxKey(
			final Configuration conf) throws IOException {
		return getKey("maxkey", conf);
	}

	private static <K extends WritableComparable<K>> K getKey(
			final String which, final Configuration conf) throws IOException {
		/* Get the serialized key */
		final String serialized = conf.get(MapFileInputFormat.class.getName()
				+ "." + which);
		if (serialized == null) {
			/*
			 * If there is no serialized key, return null (default for minimum /
			 * maximum bound)
			 */
			return null;
		}
		/*
		 * Read the base64 encoded string as a byte array, and deserialize the
		 * key
		 */
		try (DataInputStream dis = new DataInputStream(new Base64InputStream(
				new ByteArrayInputStream(serialized.getBytes(US_ASCII))))) {
			/*
			 * Get the class. This will be an unchecked cast but that is OK
			 * because a later ClassCastException is fine in case of an
			 * incorrect key type.
			 */
			@SuppressWarnings("unchecked")
			final Class<K> clazz = (Class<K>) Class.forName(dis.readUTF());
			/*
			 * Create a new key instance. For this, the default constructor must
			 * be accessible.
			 */
			final K key = clazz.newInstance();
			/* Deserialize key. */
			key.readFields(dis);
			/* Return it */
			return key;
		} catch (final ClassNotFoundException e) {
			throw new IOException("Cannot find the requested key class: "
					+ e.getMessage(), e);
		} catch (final InstantiationException e) {
			throw new IOException(
					"Cannot instantiate class. Is it abstract, an interface or has no nullary constructor? Message was: "
							+ e.getMessage(), e);
		} catch (final IllegalAccessException e) {
			throw new IOException(
					"Class or default constructor is not accessible: "
							+ e.getMessage(), e);
		}
	}
}
