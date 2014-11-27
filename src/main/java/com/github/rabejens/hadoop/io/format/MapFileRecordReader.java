package com.github.rabejens.hadoop.io.format;

import static org.apache.hadoop.io.SequenceFile.Reader.file;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

/**
 * @author jens
 *
 */
final class MapFileRecordReader<K extends WritableComparable<K>, V extends Writable>
extends SequenceFileRecordReader<K, V> {

	private final K _minKey;
	private final K _maxKey;
	private boolean _relevant;

	MapFileRecordReader(final K minKey, final K maxKey) {
		_minKey = minKey;
		_maxKey = maxKey;
	}

	@Override
	public void initialize(final InputSplit split,
			final TaskAttemptContext context) throws IOException,
			InterruptedException {
		/*
		 * Check if this split is even relevant. For this, open it with a
		 * SequenceFile reader and check first and last keys.
		 */
		final Configuration conf = context.getConfiguration();
		/*
		 * The input split is always a FileSplit, as this reader is only used by
		 * the MapFileInputFormat which only outputs FileSplits.
		 */
		final FileSplit fileSplit = (FileSplit) split;
		try (Reader r = new Reader(conf, file(fileSplit.getPath()))) {
			/*
			 * Instantiate a key. Unchecked cast is OK because
			 * ClassCastExceptions resulting from wrong key type in a file are
			 * desired.
			 */
			@SuppressWarnings("unchecked")
			final K key = (K) r.getKeyClass().newInstance();
			/* Sync to the beginning of the file split */
			final long start = fileSplit.getStart();
			r.sync(start);
			if (!r.next(key)) {
				/* Nothing to read? Split is irrelevant. */
				return;
			}
			if ((_maxKey != null) && (_maxKey.compareTo(key) <= 0)) {
				/* Max key is before or at current key? Split is irrelevant. */
				return;
			}
			if (_minKey != null) {
				/*
				 * Go to the last key, and rewind by 256 bytes until there is a
				 * key to read.
				 */
				final long end = (start + fileSplit.getLength()) - 1;
				long pos = end;
				do {
					r.sync(pos);
					pos -= 256;
				} while (!r.next(key));
				/*
				 * Now we are at a key we can check. Go through the file until
				 * we are at the split's end or ran out of keys.
				 */
				K currentLast;
				do {
					currentLast = key;
				} while (r.next(key)
						&& ((r.getPosition() <= end) || !r.syncSeen()));
				if ((currentLast != null)
						&& (_minKey.compareTo(currentLast) > 0)) {
					/*
					 * If there is a last key, and it is before the minimum key,
					 * the split is irrelevant.
					 */
					return;
				}
			}
		} catch (InstantiationException | IllegalAccessException e) {
			throw new IOException("Cannot instantiate key: " + e.getMessage(),
					e);
		}
		/* If we are here, the split is relevant. The reader can be initialized. */
		_relevant = true;
		super.initialize(split, context);
	}

	@Override
	public synchronized void close() throws IOException {
		if (_relevant) {
			super.close();
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!_relevant) {
			return false;
		}
		do {
			/* Read a value. */
			final boolean snkv = super.nextKeyValue();
			if (!snkv) {
				/* No more? Then stop. */
				return false;
			}
			if ((_maxKey != null) && (getCurrentKey().compareTo(_maxKey) >= 0)) {
				/* Past the max key? Then stop. */
				return false;
			}
			/* Repeat until the key is in our range. */
		} while ((_minKey != null) && (getCurrentKey().compareTo(_minKey) < 0));
		return true;
	}
}
