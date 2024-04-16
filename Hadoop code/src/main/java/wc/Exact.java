package wc;

import java.io.IOException;
import java.util.StringTokenizer;

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

public class Exact extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(Exact.class);

	public static class CSVMapper extends Mapper<Object, Text, Text, Text> {
		private final Text nodeId = new Text();
		private final Text edgeType = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] nodes = line.split(","); // Split the CSV line

			String firstNode = nodes[0];
			String secondNode = nodes[1];

			// System.out.println("Node 1: " + firstNode);
			// System.out.println("Node 2: " + secondNode);

			// first emit

			nodeId.set(firstNode);
			// for first node edge type is outgoing 'o'
			edgeType.set("o");

			context.write(nodeId, edgeType);

			// Second emit

			nodeId.set(secondNode);

			// for second node edge type is incoming 'i'
			edgeType.set("i");

			context.write(nodeId, edgeType);
		}
	}

	public static class ProductSumReducer extends Reducer<Text, Text, Text, Text> {
		private final Text productSum = new Text();

		@Override
		public void reduce(final Text nodeId, final Iterable<Text> edgeTypeArr, final Context context)
				throws IOException, InterruptedException {
			long incomingEdges = 0;
			long outgoingEdges = 0;

			for (Text edgeType : edgeTypeArr) {
				if (edgeType.equals(new Text("i"))) {
					incomingEdges++;
				} else {
					outgoingEdges++;
				}
			}

			long product = incomingEdges * outgoingEdges;

			productSum.set(Long.toString(product));

			// System.out.println(productSum);
			context.getCounter(CustomCounters.PRODUCT_SUM).increment(product);
			context.write(nodeId, productSum);

		}
	}

	// Global counter for product sum
	public enum CustomCounters {
		PRODUCT_SUM
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "EXACT");
		job.setJarByClass(Exact.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		// Delete output directory, only to ease local development; will not work on
		// AWS. ===========
		// final FileSystem fileSystem = FileSystem.get(conf);
		// if (fileSystem.exists(new Path(args[1]))) {
		// fileSystem.delete(new Path(args[1]), true);
		// }
		// ================
		job.setMapperClass(CSVMapper.class);

		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(ProductSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// if (job.waitForCompletion(true)) {
		// // Job completed successfully, now fetch the counters
		// Counter productSumCounter =
		// job.getCounters().findCounter(CustomCounters.PRODUCT_SUM);
		// long productSum = productSumCounter.getValue();
		// System.out.println("Product Sum: " + productSum);
		// return 0;
		// } else {
		// return 1;
		// }

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new Exact(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}