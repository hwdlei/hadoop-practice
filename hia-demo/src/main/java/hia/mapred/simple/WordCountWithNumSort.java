package hia.mapred.simple;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountWithNumSort {
	public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
		InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}

	}

	public static class IntWritableDecreasingComparator extends IntWritable.Comparator {
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);
		}

	}

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");

		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(1);
		}
		Path temp = new Path("wc_temp");
		//		conf.setBoolean("mapreduce.compress.map.output", true);
		//		conf.setBoolean("mapreduce.output.compress", true);
		//		conf.setIfUnset("mapreduce.output.compression.type", "BLOCK");
		//		conf.setClass("mapreduce.output.compression.codec", GzipCodec.class, CompressionCodec.class);
		try {
			Job job = Job.getInstance(conf, "wordcount-1");
			job.setJarByClass(WordCountWithNumSort.class);

			job.setMapperClass(Mapper1.class);
			job.setCombinerClass(Reducer1.class);
			job.setReducerClass(Reducer1.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			job.setNumReduceTasks(2);

			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, temp);

			//		System.exit(job.waitForCompletion(true) ? 0 : 1);
			boolean success = job.waitForCompletion(true);
			if (!success) {
				System.out.println("mapper1 fails!");
				System.exit(1);
			}

			Job job2 = Job.getInstance(conf, "wordcount 2");
			job2.setJarByClass(WordCountWithNumSort.class);
			job2.setMapperClass(InverseMapper.class);

			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setNumReduceTasks(1);

			// 采用默认IdentityReducer，将中间结果原样输出
			job2.setOutputKeyClass(IntWritable.class);
			job2.setOutputValueClass(Text.class);

			job2.setSortComparatorClass(IntWritableDecreasingComparator.class);
			FileInputFormat.addInputPath(job2, temp);
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		} finally {
			FileSystem.get(conf).deleteOnExit(temp);
		}
	}
}
