package yijing.assignment2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class similarityjoin_B extends Configured implements Tool {

    // define a customer counter to count the number of comparisons
	public static enum MYCOUNTER {
		totcompare_b,
	};

	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(),
				new similarityjoin_B(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job myjob = Job.getInstance(getConf(), "similarityjoin_B");

		myjob.setJarByClass(similarityjoin_B.class);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(args[1]));
		myjob.setMapperClass(Map.class);
		myjob.setReducerClass(Reduce.class);
		myjob.setMapOutputKeyClass(Text.class);
		myjob.setMapOutputValueClass(Text.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(Text.class);
		myjob.setInputFormatClass(KeyValueTextInputFormat.class);
		myjob.setOutputFormatClass(TextOutputFormat.class);
		myjob.getConfiguration().set(
				"mapreduce.input.keyvaluelinerecordreader.key.value.separator",
				",");
		myjob.getConfiguration().set(
				"mapreduce.output.textoutputformat.separator", ", ");
		myjob.setNumReduceTasks(1);

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

        // save the counter value in HDFS as a text file
		myjob.waitForCompletion(true);
		long mycounter = myjob.getCounters()
				.findCounter(MYCOUNTER.totcompare_b).getValue();
		Path outFile = new Path("totcompare_b.txt");
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
				fs.create(outFile, true)));
		br.write(String.valueOf(mycounter));
		br.close();
		return 0;
	}

	public static class Map extends Mapper<Text, Text, Text, Text> {
		private Text word = new Text();

        // define the number |d| - [0.8|d] + 1
		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] wordlength = value.toString().split(" ");
			long nb_keep = Math.round(wordlength.length
					- (wordlength.length * 0.8) + 1);
			String[] keeplength = Arrays.copyOfRange(wordlength, 0,
					(int) nb_keep);

            // output an inverted index for each selected words
			for (String keep : keeplength) {
				word.set(keep);
				context.write(word, key);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private BufferedReader reader;

        // compute the jaccard similarity (intersection/union)
		public double similarity(TreeSet<String> str1, TreeSet<String> str2) {

			if (str1.size() < str2.size()) {
				TreeSet<String> s1bis = str1;
				s1bis.retainAll(str2);
				int intersection = s1bis.size();
				str1.addAll(str2);
				int union = str1.size();
				return (double) intersection / union;
			} else {
				TreeSet<String> s1bis = str2;
				s1bis.retainAll(str1);
				int intersection = s1bis.size();
				str2.addAll(str1);
				int union = str2.size();
				return (double) intersection / union;
			}

		}

        // load the word list (without frequency) into a HashMap
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			HashMap<String, String> lineshashmap = new HashMap<String, String>();
			reader = new BufferedReader(
					new FileReader(
							new File(
									"/home/cloudera/workspace/assignment2/output/wordlist_asc.txt")));
			String pattern;
			while ((pattern = reader.readLine()) != null) {
				String[] line = pattern.split(",");
				lineshashmap.put(line[0], line[1]);
			}

			ArrayList<String> wordlength = new ArrayList<String>();

			for (Text word : values) {
				wordlength.add(word.toString());
			}

            //If a word is not present in only one document, we get all possible pairs
			if (wordlength.size() > 1) {
				ArrayList<String> pairs = new ArrayList<String>();
				for (int i = 0; i < wordlength.size(); ++i) {
					for (int j = i + 1; j < wordlength.size(); ++j) {
						String pair = new String(wordlength.get(i) + " "
								+ wordlength.get(j));
						pairs.add(pair);
					}
				}

                // create 2 sets to compute the similiarity
				for (String pair : pairs) {
					TreeSet<String> words_1_treeset = new TreeSet<String>();
					String words_1_string = lineshashmap
							.get(pair.split(" ")[0].toString());
					for (String word : words_1_string.split(" ")) {
						words_1_treeset.add(word);
					}

					TreeSet<String> words_2_treeset = new TreeSet<String>();
					String words_2_string = lineshashmap
							.get(pair.split(" ")[1].toString());
					for (String word : words_2_string.split(" ")) {
						words_2_treeset.add(word);
					}
                    
                    // compute the jaccard similarity between 2 sets
					double sim = similarity(words_1_treeset, words_2_treeset);
                    
                    
                    // increment the counter by 1
                    context.getCounter(MYCOUNTER.totcompare_b).increment(1);

                    // Assuming threshold t = 0.8
					if (sim >= 0.8) {
						context.write(new Text("(" + pair.split(" ")[0] + ", "
								+ pair.split(" ")[1] + ")"),
								new Text(String.valueOf(sim)));
					}
				}
			}
		}
	}
}
