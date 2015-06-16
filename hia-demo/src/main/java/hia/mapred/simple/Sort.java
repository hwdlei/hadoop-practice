package hia.mapred.simple;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Sort {
	private static String LINENUM = "line.num";
	public static class SortMap extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int a = Integer.valueOf(value.toString());
			context.write(new IntWritable(a), new IntWritable(1));
		}
	}

	public static class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private static IntWritable lineNum = new IntWritable(1);

		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			for(IntWritable value : values) {
				int lineNum = conf.getInt(LINENUM, 1);
				context.write(key, new IntWritable(lineNum));
//				lineNum = new IntWritable(lineNum.get() + 1);
				conf.setInt(LINENUM, lineNum + 1);
			}
		}
	}

	public static class Partition extends Partitioner<IntWritable, IntWritable> {

		@Override
		public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
			int MaxNum = 65535;
			int interval = MaxNum / numPartitions + 1;
			int keyNum = key.get();
			for (int i = 0; i < numPartitions; i++) {
				if (keyNum < interval * (i + 1) && keyNum >= interval * i) {
					return i;
				}
			}
			return -1;
		}

	}

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt(LINENUM, 1);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage");
			System.exit(1);
		}
		Job job = new Job(conf, "Sort");
		job.setJarByClass(Sort.class);
		job.setMapperClass(SortMap.class);
		job.setReducerClass(SortReducer.class);
		job.setPartitionerClass(Partition.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
