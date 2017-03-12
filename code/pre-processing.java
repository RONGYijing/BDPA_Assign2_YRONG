package yijing.assignment2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

public class pre-processing extends Configured implements Tool {

	private static final Logger LOG = Logger
			.getLogger(pre-processing.class);

    // Define a custom counter to count the number of output records
	public static enum MYCOUNTER {
		totlines,
	};

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new pre-processing(), args);
		System.exit(res);
	}


	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), "pre-processing");

        // additional configurations to handle the "-pass" method
		for (int i = 0; i < args.length; i += 1) {
			if ("-pass".equals(args[i])) {
				job.getConfiguration().setBoolean(
						"pre-processing.pass.patterns", true);
				i += 1;
				job.addCacheFile(new Path(args[i]).toUri());
				LOG.info("Added file to the distributed cache: " + args[i]);
			}
		}

		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.getConfiguration().set(
				"mapreduce.output.textoutputformat.separator", ", ");
		job.setNumReduceTasks(1);

		FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

        // Store the counter in HDFS until the job is complete
		job.waitForCompletion(true);
		long mycounter = job.getCounters().findCounter(MYCOUNTER.totlines)
				.getValue();
		Path outFile = new Path("totlines.txt");
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
				fs.create(outFile, true)));
		bw.write(String.valueOf(mycounter));
		bw.close();
		return 0;
	}

	public static class Map extends
			Mapper<LongWritable, Text, LongWritable, Text> {
		private Set<String> patternsToRemove = new HashSet<String>();
		private BufferedReader fileinput;

                
        // "loadfile" method to load the stopwords file
		protected void loadfile(Mapper.Context context) throws IOException,
				InterruptedException {
			if (context.getInputSplit() instanceof FileSplit) {
				((FileSplit) context.getInputSplit()).getPath().toString();
			} else {
				context.getInputSplit().toString();
			}
			Configuration cfgr = context.getConfiguration();
			if (cfgr.getBoolean("pre-processing.pass.patterns", false)) {
				URI[] localPaths = context.getCacheFiles();
				parsefile(localPaths[0]);
			}
		}

        // "parsefile" method to parse the file
		private void parsefile(URI patternsURI) {
			LOG.info("Added file to the distributed cache: " + patternsURI);
			try {
				fileinput = new BufferedReader(new FileReader(new File(
						patternsURI.getPath()).getName()));
				String pattern;
				while ((pattern = fileinput.readLine()) != null) {
					patternsToRemove.add(pattern);
				}
			} catch (IOException ioe) {
				System.err
						.println("Exception while parsing the cached file '"
								+ patternsURI
								+ "' : "
								+ StringUtils.stringifyException(ioe));
			}
		}

		public void map(LongWritable key, Text mytext, Context mycontext)
				throws IOException, InterruptedException {

			for (String word : mytext.toString().split("\\s*\\b\\s*")) {

                // filter away any strings that don’t have [a-z],[A-Z]and[0-9]
				Pattern patt = Pattern.compile("[^A-Za-z0-9]");

				if (mytext).toString().length() == 0
                // condition of empty lines
                    
                        // remove the stopwords list
						|| patternsToRemove.contains(word.toLowerCase())
                    
                        // ensure the empty words and special characters are removed
                        || word.toLowerCase().isEmpty()
						|| patt.matcher(word.toLowerCase()).find()) {
                            
					continue;
				}

				mycontext.write(key, new Text(word.toLowerCase()));
			}
		}
	}

	public static class Reduce extends
			Reducer<LongWritable, Text, LongWritable, Text> {

		private BufferedReader reader;

		@Override
		public void reduce(LongWritable key, Iterable<Text> mytexts,
				Context mycontext) throws IOException, InterruptedException {

            // Keep each unique word only once per line
			ArrayList<String> wordlist = new ArrayList<String>();
            
            for (Text word : mytexts) {
                wordlist.add(word.toString());
            }
            
            
            // load the wordcounting file which have the global frequency of each word
			HashMap<String, String> wordcounting = new HashMap<String, String>();
			reader = new BufferedReader(
					new FileReader(
							new File(
									"/home/cloudera/workspace/assignment2/output/wordcounting.txt")));
			String pattern;
			while ((pattern = reader.readLine()) != null) {
				String[] word = pattern.split(",");
				wordcounting.put(word[0], word[1]);
			}

            HashSet<String> hashsets = new HashSet<String>(wordlist);

			StringBuilder wordstringbuilder = new StringBuilder();

            // add global frequency of each word computed in the wordcounting.java
			String pr_1 = "";
			for (String word : hashsets) {
				wordstringbuilder.append(pr_1);
				pr_1 = ", ";
				wordstringbuilder.append(word + "#" + wordcounting.get(word));
			}

            // sort the word list in ascending order
			java.util.List<String> wordcountlist = Arrays
					.asList(wordstringbuilder.toString().split("\\s*,\\s*"));

			Collections.sort(wordcountlist, new Comparator<String>() {
				public int compare(String freq1, String freq2) {
					return extractInt(freq1) - extractInt(freq2);
				}

				int extractInt(String str) {
					String nb = str.replaceAll("[^#\\d+]", "");
					nb = nb.replaceAll("\\d+#", "");
					nb = nb.replaceAll("#", "");
					return nb.isEmpty() ? 0 : Integer.parseInt(nb);
				}
			});

			StringBuilder wordstringbuilder_asc = new StringBuilder();

			String pr_2 = "";
			for (String word : wordcountlist) {
				wordstringbuilder_asc.append(pr_2);
				pr_2 = ", ";
				wordstringbuilder_asc.append(word);
			}

            // increment the counter by 1
			mycontext.getCounter(MYCOUNTER.totlines).increment(1);

			mycontext.write(key, new Text(wordstringbuilder_asc.toString()));

		}
	}
}
