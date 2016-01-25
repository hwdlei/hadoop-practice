package hia.mapred.simple;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SingleTableJoin {

	public static class STMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			if (!line.contains("child") && !line.contains("parent")) {
				StringTokenizer tokens = new StringTokenizer(line);
				while (tokens.hasMoreTokens()) {
					String child = tokens.nextToken();
					if (tokens.hasMoreTokens()) {
						String parent = tokens.nextToken();
						context.write(new Text(child), new Text("r_" + parent));
						context.write(new Text(parent), new Text("l_" + child));
					}
				}
			}
		}
	}

	public static class STReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
		InterruptedException {
			List<String> grandChilds = new ArrayList<String>();
			List<String> grandParents = new ArrayList<String>();
			for (Text value : values) {
				String tmp = value.toString();
				int i = tmp.indexOf("_");

				if (tmp.substring(0, i).equals("r")) {
					grandParents.add(tmp.substring(i + 1));
				} else {
					grandChilds.add(tmp.substring(i + 1));
				}
			}

			for (String parentChild : grandChilds) {
				for (String parentParent : grandParents) {
					context.write(new Text(parentChild), new Text(parentParent));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.exit(1);
		}
		Job job = Job.getInstance(conf, "stjoin");
		job.setJarByClass(SingleTableJoin.class);
		job.setMapperClass(STMapper.class);
		job.setReducerClass(STReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
