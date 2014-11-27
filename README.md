MapFileInputFormat
==================

A Hadoop InputFormat for MapFiles which filters irrelevant FileSplits before passing anything to a mapper.

Purpose
-------

Suppose you have some very large files with sorted keys in your file system, and the keys are sorted.
When writing a MapReduce job, you might sometimes only want to use a fraction of the input data.
The usual approach is to write a mapper which only processes the relevant records.

But if you have to skip a large amount of data, this can cause significant slowdowns,
especially if this large amount has to be transmitted from other nodes.

This MapFileInputFormat only checks the first and last keys of an InputSplit
to quickly determine if this split is relevant, not outputting anything if not.

Usage
-----

Usage is straightforward. Just use it like any other input format:

```java
job.setInputFormatClass(MapFileInputFormat.class);
```

This will also work when recursion is turned on - the default SequenceFileInputFormat
runs into problems with MapFiles (see [MAPREDUCE-6155](https://issues.apache.org/jira/browse/MAPREDUCE-6155))

The above example would use all data from MapFiles into the supplied input directories.

To just use a fraction, use the helper methods `setMinKey` and `setMaxKey`.
Suppose your key class is `IntWritable`:
```java
Configuration conf = job.getConfiguration();
IntWritable k = new IntWritable();
k.set(1000);
MapFileInputFormat.setMinKey(k, conf);
k.set(5000000);
MapFileInputFormat.setMaxKey(k, conf);
```

This would tell the input format to only process portions of the MapFiles where
the keys are between 1000 (inclusive) and 5000000 (exclusive). You can reuse
the key for setMinKey and setMaxKey because it will be serialized into the
configuration.