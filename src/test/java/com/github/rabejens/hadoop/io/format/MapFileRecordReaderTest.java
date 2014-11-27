package com.github.rabejens.hadoop.io.format;

import static org.apache.hadoop.io.MapFile.DATA_FILE_NAME;
import static org.apache.hadoop.io.MapFile.Writer.keyClass;
import static org.apache.hadoop.io.MapFile.Writer.valueClass;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;

/**
 * @author jens
 *
 */
@RunWith(Parameterized.class)
public class MapFileRecordReaderTest {

	private static Configuration _conf;
	private static FileSystem _fs;
	private static Path _testDir;
	private static Path _inDir;
	private static List<FileSplit> _splits;

	private final IntWritable _from;
	private final IntWritable _to;

	@Mock
	private TaskAttemptContext _context;

	public MapFileRecordReaderTest(final Integer from, final Integer to) {
		_from = (from == null) ? null : new IntWritable(from);
		_to = (to == null) ? null : new IntWritable(to);
	}

	@Parameters
	public static List<Object[]> params() {
		return Arrays.asList(new Object[] { null, 1338 }, new Object[] { 1337,
				31338 }, new Object[] { 31337, null }, new Object[] { null,
				null });
	}

	@BeforeClass
	public static void setup() throws IOException {
		_conf = new Configuration();
		_fs = FileSystem.get(_conf);
		do {
			_testDir = new Path(UUID.randomUUID().toString());
		} while (_fs.exists(_testDir));
		_inDir = new Path(_testDir, "in");
		_fs.mkdirs(_inDir);
		final Path sampleFile = new Path(_inDir, "sample");
		try (Writer w = new Writer(_conf, sampleFile,
				keyClass(IntWritable.class), valueClass(Text.class))) {
			final IntWritable k = new IntWritable();
			final Text v = new Text();
			for (int i = 0; i < 1048576; i++) {
				k.set(i);
				v.set("Value " + i);
				w.append(k, v);
			}
		}
		_splits = new ArrayList<>();
		final Path dataFile = new Path(sampleFile, DATA_FILE_NAME);
		final FileStatus dfs = _fs.getFileStatus(dataFile);
		for (long start = 0, len = dfs.getLen(); start < len; start += 131072) {
			final long splitLen = Math.min(start + 131072, len) - start;
			_splits.add(new FileSplit(dataFile, start, splitLen, null));
		}
	}

	@AfterClass
	public static void teardown() throws IOException {
		try {
			if ((_fs != null) && (_testDir != null) && _fs.exists(_testDir)) {
				_fs.delete(_testDir, true);
			}
		} finally {
			if (_fs != null) {
				_fs.close();
			}
		}
	}

	@Before
	public void setupContext() {
		initMocks(this);
		when(_context.getConfiguration()).thenReturn(_conf);
	}

	@Test
	public void filtersCorrectly() throws IOException, InterruptedException {
		final Map<Integer, Integer> kvCounts = new HashMap<>();
		for (final FileSplit split : _splits) {
			try (MapFileRecordReader<IntWritable, Text> mfrr = new MapFileRecordReader<IntWritable, Text>(
					_from, _to)) {
				mfrr.initialize(split, _context);
				while (mfrr.nextKeyValue()) {
					final int k = mfrr.getCurrentKey().get();
					final Integer count = kvCounts.get(k);
					if (count == null) {
						kvCounts.put(k, 1);
					} else {
						kvCounts.put(k, count + 1);
					}
				}
			}
		}
		final IntWritable iw = new IntWritable();
		final Integer one = Integer.valueOf(1);
		for (int i = 0; i < 1048576; i++) {
			iw.set(i);
			final Integer count = kvCounts.get(i);
			if (((_from == null) || (iw.compareTo(_from) >= 0))
					&& ((_to == null) || (iw.compareTo(_to) < 0))) {
				assertThat(String.format("[%s, %s) - value %d appeared once",
						_from, _to, i), count, is(one));
			} else {
				assertThat(String.format("[%s, %s) - value %d did not appear",
						_from, _to, i), count, nullValue());
			}
		}
	}
}
