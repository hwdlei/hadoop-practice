package hia.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileReadDemo {
	public static void main(String[] args) throws IOException {
		String uri = "2016-01-23";
		Configuration conf = new Configuration();
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(conf, Reader.file(new Path(uri)),Reader.bufferSize(1024));
			LongWritable key = (LongWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();
			while (reader.next(key, value)) {
				String syncSeen = reader.syncSeen() ? "*" : "";
				System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
				position = reader.getPosition();
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}
