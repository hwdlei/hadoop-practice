package hia.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

/**
 * MapFile的写入的数据是升序的
 * 生成的是文件夹：data存放的是数据，index是索引
 * 索引间隔默认是128，可由io.map.index.interval设置
 * @author donglei
 *
 */

public class MapFileWriteDemo {
	private static String[] myValue = { "hello world", "bye world", "hello hadoop", "bye hadoop" };

	public static void main(String[] args) throws IOException {
		String uri = "io/map";
		Configuration conf = new Configuration();
		MapFile.Writer writer = null;
		IntWritable key = new IntWritable();
		Text text = new Text();
		try {
			writer = new MapFile.Writer(conf,new Path(uri),Writer.bufferSize(1024));
			for (int i = 0; i < 500; i++) {
				key.set(i);
				text.set(myValue[i % myValue.length]);
				writer.append(key, text);
			}
			//  wrong: MapFile的写入的数据是生序的
			//			key.set(1);
			//			text.set("test");
			//			writer.append(key, text);
		} finally {
			IOUtils.closeStream(writer);
		}

	}

}
