package yijing.assignment2;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class wordcounting extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job myjob = Job.getInstance(getConf(), "wordcounting");

		myjob.setJarByClass(wordcounting.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(IntWritable.class);

		myjob.setMapperClass(Map.class);
		myjob.setReducerClass(Reduce.class);

		myjob.setInputFormatClass(TextInputFormat.class);
		myjob.setOutputFormatClass(TextOutputFormat.class);

		myjob.getConfiguration().set(
				"mapreduce.output.textoutputformat.separator", ",");
		myjob.setNumReduceTasks(1);

		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(args[1]));

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		myjob.waitForCompletion(true);

		return 0;
	}

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text mytext, Context mycontext)
				throws IOException, InterruptedException {
			for (String word : mytext.toString().split("\\s*\\b\\s*")) {

				Pattern p = Pattern.compile("[^A-Za-z0-9]");

				if (word.toLowerCase().isEmpty()
						|| p.matcher(word.toLowerCase()).find()) {
					continue;
				}
				mycontext.write(new Text(word.toLowerCase()), ONE);
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> mytexts,
				Context mycontext) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : mytexts) {
				sum += val.get();
			}
			mycontext.write(key, new IntWritable(sum));
		}
	}
}
