package hia.io;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileSystemCat {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			FSDataInputStream in = null;
			try {
				FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
				in = fs.open(new Path(args[0]));
				IOUtils.copyBytes(in, System.out, 4096, false);
				in.seek(4);
				IOUtils.copyBytes(in, System.out, 4096, false);
			} finally {
				IOUtils.closeStream(in);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
