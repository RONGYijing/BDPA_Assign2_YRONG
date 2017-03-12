package yijing.assignment2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.WritableComparable;

class TextPair implements WritableComparable<TextPair> {

    // add the pair objects as textpair fields
	private Text first;
	private Text second;
    

    // implement the set method that changes the pair content
	public void set(Text first, Text second) {
		this.first = first;
		this.second = second;
	}
    
    
    // implement the first getter
    public Text getFirst() {
        return first;
    }
    
    // implement the second getter
    public Text getSecond() {
        return second;
    }
    
    // implement the constructor
    public TextPair(Text first, Text second) {
        set(first, second);
    }
    
    public TextPair() {
        set(new Text(), new Text());
    }
    
    public TextPair(String first, String second) {
        set(new Text(first), new Text(second));
    }


    // write to out the serialized version of this such that can be deserialized in future
    // this will be use to write to HDFS
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

    // read from in the serialized version of a pair and deserialize it
    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }
    
    // implement hash
    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }
    
    // implement equals
    @Override
    public boolean equals(Object o) {
        if (o instanceof TextPair) {
            TextPair tp = (TextPair) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }

    // implement the comparison between this and other
	@Override
	public int compareTo(TextPair other) {
		int cmpFirstFirst = first.compareTo(other.first);
		int cmpSecondSecond = second.compareTo(other.second);
		int cmpFirstSecond = first.compareTo(other.second);
		int cmpSecondFirst = second.compareTo(other.first);

		if (cmpFirstFirst == 0 && cmpSecondSecond == 0 || cmpFirstSecond == 0
				&& cmpSecondFirst == 0) {
			return 0;
		}

		Text thisSmaller;
		Text otherSmaller;

		Text thisBigger;
		Text otherBigger;

		if (this.first.compareTo(this.second) < 0) {
			thisSmaller = this.first;
			thisBigger = this.second;
		} else {
			thisSmaller = this.second;
			thisBigger = this.first;
		}

		if (other.first.compareTo(other.second) < 0) {
			otherSmaller = other.first;
			otherBigger = other.second;
		} else {
			otherSmaller = other.second;
			otherBigger = other.first;
		}

		int cmpThisSmallerOtherSmaller = thisSmaller.compareTo(otherSmaller);
		int cmpThisBiggerOtherBigger = thisBigger.compareTo(otherBigger);

		if (cmpThisSmallerOtherSmaller == 0) {
			return cmpThisBiggerOtherBigger;
		} else {
			return cmpThisSmallerOtherSmaller;
		}
	}

    
    // implement toString for text output format
    @Override
    public String toString() {
        return first + "\t" + second;
    }
}

public class similarityjoin_A extends Configured implements
		Tool {

    // define a customer counter to count the number of comparisons
	public static enum MYCOUNTER {
		totcompare_a,
	};

	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(),
				new similarityjoin_A(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job myjob = Job.getInstance(getConf(), "similarityjoin_A");

		myjob.setJarByClass(similarityjoin_A.class);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(args[1]));
		myjob.setMapperClass(Map.class);
		myjob.setReducerClass(Reduce.class);
		myjob.setMapOutputKeyClass(TextPair.class);
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
				.findCounter(MYCOUNTER.totcompare_a).getValue();
		Path outFile = new Path("totcompare_a.txt");
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
				fs.create(outFile, true)));
		br.write(String.valueOf(mycounter));
		br.close();
		return 0;
	}

	public static class Map extends Mapper<Text, Text, TextPair, Text> {

		private BufferedReader reader;
		private static TextPair textPair = new TextPair();

        // load the wordcount list (without frequency)
		@Override
		public void map(Text key, Text value, Context context)
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

            // document id as key element, document content as value
			for (String line : lineshashmap.keySet()) {
				if (key.toString().equals(line)) {
					continue;
				}

				textPair.set(key, new Text(line));
				context.write(textPair, new Text(value.toString()));
			}
		}
	}

	public static class Reduce extends Reducer<TextPair, Text, Text, Text> {

		private BufferedReader reader;

        // compute the jaccard similarity
		public double similarity (TreeSet<String> str1, TreeSet<String> str2) {
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

		@Override
		public void reduce(TextPair key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

            // load the word list (without frequency) into a HashMap
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

            // take as values the words list of doc2 in the pair of (doc1, doc2)
			TreeSet<String> words_2_treeset = new TreeSet<String>();
			String words_2_string = lineshashmap.get(key.getSecond()
					.toString());
			for (String word : words_2_string.split(" ")) {
				words_2_treeset.add(word);
			}

            // take as values the words list of doc1 in the pair of (doc1, doc2)
			TreeSet<String> words_1_treeset = new TreeSet<String>();

			for (String word : values.iterator().next().toString().split(" ")) {
				words_1_treeset.add(word);
			}

            // increment the counter by 1
			context.getCounter(MYCOUNTER.totcompare_a).increment(1);
            
            // compute the jaccard similarity between 2 sets
			double sim = similarity(words_1_treeset, words_2_treeset);

            // Assuming threshold t = 0.8
			if (sim >= 0.8) {
				context.write(new Text("(" + key.getFirst() + ", " + key.getSecond() + ")"),
						new Text(String.valueOf(sim)));
			}
		}
	}
}
