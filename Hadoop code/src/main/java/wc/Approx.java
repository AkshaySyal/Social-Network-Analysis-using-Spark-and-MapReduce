package wc;

import java.io.IOException;
import java.util.StringTokenizer;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
// import wc.WordCount.TokenizerMapper;

public class Approx extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(Approx.class);

	public static class CSVMapperClass extends Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context)
				throws IOException, InterruptedException {

			int max = Integer.parseInt(context.getConfiguration().get("MAX"));
			//System.out.println(max);
			String line = value.toString();
			String[] nodes = line.split(","); // Split the CSV line

			String followerNode = nodes[0];
			String followedNode = nodes[1];

			// System.out.println("Node 1: " + firstNode);
			// System.out.println("Node 2: " + secondNode);

			// first emit
			//
			int followedNodeId = Integer.parseInt(followedNode);
			int followerNodeId = Integer.parseInt(followerNode);

			if (followedNodeId < max && followerNodeId < max) {

				outkey.set(followerNode);
				outvalue.set("E" + followedNode);
				context.write(outkey, outvalue);

				// Second emit
				//
				outkey.set(followedNode);
				outvalue.set("S" + followerNode);
				context.write(outkey, outvalue);
			}

			// context.write(new Text(followedNode), new Text("R" + followerNode));
		}
	}

	public static class ApproxTwoPathReducer extends Reducer<Text, Text, Text, Text> {
		private ArrayList<Text> startList = new ArrayList<Text>();
		private ArrayList<Text> endList = new ArrayList<Text>();
		private Text outvalue = new Text();

		@Override
		public void reduce(final Text midNode, final Iterable<Text> records, final Context context)
				throws IOException, InterruptedException {

			startList.clear();
			endList.clear();

			for (Text rec : records) {

				if (rec.charAt(0) == 'S') {
					startList.add(new Text(rec.toString().substring(1)));
				} else {
					endList.add(new Text(rec.toString().substring(1)));
				}
			}

			for (Text startNode : startList) {
				for (Text endNode : endList) {
					context.getCounter(CustomCounters.TWO_PATH_COUNTER).increment(1);
					outvalue.set(midNode.toString() + " " + endNode.toString());
					context.write(startNode, outvalue);
				}
			}

		}
	}

	// Global counter for product sum
	public enum CustomCounters {
		TWO_PATH_COUNTER
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Approx");
		job.setJarByClass(Approx.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		// Delete output directory, only to ease local development; will not work on
		// AWS. ===========
		// final FileSystem fileSystem = FileSystem.get(conf);
		// if (fileSystem.exists(new Path(args[1]))) {
		// fileSystem.delete(new Path(args[1]), true);
		// }
		// ================
		job.setMapperClass(CSVMapperClass.class);

		// job.setCombinerClass(ApproxTwoPathReducer.class);
		job.setReducerClass(ApproxTwoPathReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		String max = args[2];
		job.getConfiguration().set("MAX", max);

		// Counter twoPathCounter =
		// job.getCounters().findCounter(CustomCounters.TWO_PATH_COUNTER);
		// long pathCount = twoPathCounter.getValue();
		// System.out.println("2 Path Count: " + pathCount);

		return job.waitForCompletion(true) ? 0 : 1;
		// if (job.waitForCompletion(true)) {
		// // Job completed successfully, now fetch the counters
		// Counter twoPathCounter =
		// job.getCounters().findCounter(CustomCounters.TWO_PATH_COUNTER);
		// long pathCount = twoPathCounter.getValue();
		// System.out.println("2 Path Count: " + pathCount);
		// return 0;
		// } else {
		// return 1;
		// }

		// return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			// MAX PARAM
			// Filter f = new Filter();
			// f.filter(62500);
			ToolRunner.run(new Approx(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}