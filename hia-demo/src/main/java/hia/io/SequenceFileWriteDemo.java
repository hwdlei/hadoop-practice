package hia.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

public class SequenceFileWriteDemo {
	private static String[] myValue = { "hello world", "bye world", "hello hadoop", "bye hadoop" };

	public static void main(String[] args) throws IOException {
		String uri = "io/sequence";
		Configuration conf = new Configuration();
		SequenceFile.Writer writer = null;
		IntWritable key = new IntWritable();
		Text text = new Text();
		try {
			writer = SequenceFile.createWriter(conf, Writer.file(new Path(uri)), Writer.keyClass(LongWritable.class),
					Writer.valueClass(BytesWritable.class), Writer.bufferSize(1024),Writer.compression(CompressionType.NONE));
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
