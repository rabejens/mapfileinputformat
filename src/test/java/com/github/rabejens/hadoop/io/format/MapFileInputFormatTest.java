package com.github.rabejens.hadoop.io.format;

import static com.github.rabejens.hadoop.io.format.MapFileInputFormat.getMaxKey;
import static com.github.rabejens.hadoop.io.format.MapFileInputFormat.getMinKey;
import static com.github.rabejens.hadoop.io.format.MapFileInputFormat.setMaxKey;
import static com.github.rabejens.hadoop.io.format.MapFileInputFormat.setMinKey;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

/**
 * @author jens
 *
 */
public class MapFileInputFormatTest {

	@Test
	public void setThenGetMinKeyNull() throws IOException {
		final Configuration conf = new Configuration();
		setMinKey(null, conf);
		final WritableComparable<?> k = getMinKey(conf);
		assertThat("Setting and then getting a null Min key returns null", k,
				nullValue());
	}

	@Test
	public void setThenGetMaxKeyNull() throws IOException {
		final Configuration conf = new Configuration();
		setMaxKey(null, conf);
		final WritableComparable<?> k = getMaxKey(conf);
		assertThat("Setting and then getting a null Max key returns null", k,
				nullValue());
	}

	@Test
	public void setThenGetMinKey() throws IOException {
		final Configuration conf = new Configuration();
		setMinKey(new IntWritable(42), conf);
		final IntWritable k = getMinKey(conf);
		assertThat("Setting and then getting 42 as a Min key returns 42",
				k.get(), is(42));
	}

	@Test
	public void setThenGetMaxKey() throws IOException {
		final Configuration conf = new Configuration();
		setMaxKey(new IntWritable(42), conf);
		final IntWritable k = getMaxKey(conf);
		assertThat("Setting and then getting 42 as a Max key returns 42",
				k.get(), is(42));
	}

	@Test
	public void getStatuses() throws IOException {
		final Configuration conf = new Configuration();
		try (FileSystem fs = FileSystem.get(conf)) {
			Path testDir = null;
			try {
				do {
					testDir = new Path(UUID.randomUUID().toString());
				} while (fs.exists(testDir));
				final Path inDir = new Path(testDir, "in");
				fs.mkdirs(inDir);
				for (int i = 0; i < 3; i++) {
					try (MapFile.Writer w = new MapFile.Writer(conf, new Path(
							inDir, "map" + i),
							MapFile.Writer.keyClass(IntWritable.class),
							MapFile.Writer.valueClass(Text.class))) {
						w.append(new IntWritable(i), new Text("foo"));
					}
					try (SequenceFile.Writer w = SequenceFile.createWriter(
							conf, SequenceFile.Writer.file(new Path(inDir,
									"seq" + i)), SequenceFile.Writer
									.keyClass(IntWritable.class),
							SequenceFile.Writer.valueClass(Text.class))) {
						w.append(new IntWritable(i), new Text("foo"));
					}
					final Path d = new Path(inDir, "dir" + i);
					fs.mkdirs(d);
					try (SequenceFile.Writer w = SequenceFile.createWriter(
							conf,
							SequenceFile.Writer.file(new Path(d, "seq" + i)),
							SequenceFile.Writer.keyClass(IntWritable.class),
							SequenceFile.Writer.valueClass(Text.class))) {
						w.append(new IntWritable(i), new Text("foo"));
					}
				}
				final List<FileStatus> statuses = MapFileInputFormat
						.getMapFileStatuses(asList(fs.listStatus(inDir)), conf);
				assertThat("There are three map files", statuses.size(), is(3));
				final Set<String> names = new HashSet<>();
				for (final FileStatus s : statuses) {
					names.add(s.getPath().getParent().getName());
				}
				assertThat(
						"They are called map0, map1, and map2",
						names,
						is((Set<String>) new HashSet<>(asList("map0", "map1",
								"map2"))));
			} finally {
				if ((testDir != null) && fs.exists(testDir)) {
					fs.delete(testDir, true);
				}
			}
		}
	}
}
