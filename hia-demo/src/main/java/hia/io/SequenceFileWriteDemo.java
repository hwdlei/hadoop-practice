package hia.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;

public class SequenceFileWriteDemo {
	private static String[] myValue = { "hello world", "bye world", "hello hadoop", "bye hadoop" };

	public static void main(String[] args) throws IOException {
		String uri = "io/sequence";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Writer writer = null;
		IntWritable key = new IntWritable();
		Text text = new Text();
		try {
			//			writer = SequenceFile.createWriter(fs, conf, new Path(uri), IntWritable.class, Text.class);
			writer = SequenceFile.createWriter(fs, conf, new Path(uri), IntWritable.class, Text.class,
					CompressionType.BLOCK);
			for (int i = 0; i < 2000000; i++) {
				key.set(i);
				text.set(myValue[i % myValue.length]);
				writer.append(key, text);
			}
		} finally {
			IOUtils.closeStream(writer);
		}

	}

}
