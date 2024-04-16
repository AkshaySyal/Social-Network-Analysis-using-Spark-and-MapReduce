package wc;

// Libraries req for rep join
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;



import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

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

import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
// import wc.WordCount.TokenizerMapper;

public class RepJoin extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(RepJoin.class);

	public static class RepJoinMapper extends Mapper<Object, Text, Text, Text> {
		private Text outvalue = new Text();

		private HashMap<Text, List<Text>> hmap = new HashMap<Text, List<Text>>();

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {

			// New code
			URI[] cacheFiles = context.getCacheFiles();
			logger.info("cacheFiles: " + cacheFiles[0].getPath().toString());

			if (cacheFiles == null || cacheFiles.length == 0) {
				throw new RuntimeException("User information is not set in DistributedCache");
			} else {
				logger.info("cacheFiles: " + cacheFiles[0].getPath().toString());
			}

			if (cacheFiles != null && cacheFiles.length > 0) {
				String str;
				// remove this comment when running on EMR /**
				// FileSystem fs = FileSystem.get(context.getConfiguration());
				// fs.setWorkingDirectory(new Path("/sample_input/"));
				// FSDataInputStream in = fs.open(new Path("edges_3_nodes_complete_graph.csv"));
				// logger.info("in: " + in.toString());
				// BufferedReader reader = new BufferedReader(new InputStreamReader(in));
				// **/
				// remove this comment when running on local
				BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].getPath().toString()+"/edges_3_nodes_complete_graph.csv"));
				while ((str = reader.readLine()) != null) {
					// String[] tokens = str.split(",");
					String[] nodes = str.split(",");
					if (nodes.length >= 2 && !nodes[0].isEmpty() && !nodes[1].isEmpty()) {
						Text startNode = new Text(nodes[0]);
						Text endNode = new Text(nodes[1]);

						int max = Integer.parseInt(context.getConfiguration().get("MAX"));
						int startNodeId = Integer.parseInt(startNode.toString());
						int endNodeId = Integer.parseInt(endNode.toString());
						// followedNodeId < max && followerNodeId < max

						if (startNodeId < max && endNodeId < max) {
							if (!hmap.containsKey(startNode)) {
								hmap.put(startNode, new ArrayList<Text>());
							}
							hmap.get(startNode).add(endNode);
						}

					}
				}
			}

			// for (URI file : files) {
			// if (file.getPath().contains("edge")) {
			// Path path = new Path(file);
			// BufferedReader reader = new BufferedReader(new FileReader(path.getName()));
			// String line = reader.readLine();
			// while (line != null) {
			// String[] nodes = line.split(",");
			// Text startNode = new Text(nodes[0]);
			// Text endNode = new Text(nodes[1]);

			// if (!hmap.containsKey(startNode)) {
			// hmap.put(startNode, new ArrayList<Text>());
			// }
			// hmap.get(startNode).add(endNode);
			// }
			// }
			// }
			// End of new code

			// Path[] files = DistributedCache.getLocalCacheFiles(context
			// .getConfiguration());

			// if (files == null || files.length == 0) {
			// throw new RuntimeException(
			// "User information is not set in DistributedCache");
			// }

			// // Read all files in the DistributedCache
			// for (Path p : files) {
			// BufferedReader rdr = new BufferedReader(
			// new InputStreamReader(
			// new GZIPInputStream(new FileInputStream(
			// new File(p.toString())))));

			// String line;
			// // For each line in csv file
			// while ((line = rdr.readLine()) != null) {
			// String[] nodes = line.split(",");
			// Text startNode = new Text(nodes[0]);
			// Text endNode = new Text(nodes[1]);

			// if (!hmap.containsKey(startNode)) {
			// hmap.put(startNode, new ArrayList<Text>());
			// }
			// hmap.get(startNode).add(endNode);

			// }
			// }

		}

		@Override
		public void map(final Object key, final Text value, final Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] nodes = line.split(","); // Split the CSV line

			Text edgeStartNode = new Text(nodes[0]);
			Text edgeEndNode = new Text(nodes[1]);

			int max = Integer.parseInt(context.getConfiguration().get("MAX"));
			int edgeStartNodeId = Integer.parseInt(edgeStartNode.toString());
			int edgeEndNodeId = Integer.parseInt(edgeEndNode.toString());
			// followedNodeId < max && followerNodeId < max

			// if (startNodeId < max && endNodeId < max)

			if (edgeStartNodeId < max && edgeEndNodeId < max && hmap.containsKey(edgeEndNode)) {
				List<Text> hmapEndNodes = hmap.get(edgeEndNode);

				for (Text hmapEndNode : hmapEndNodes) {
					// Avoiding loop like paths (2,1,2)
					if (!hmapEndNode.equals(edgeStartNode)) {
						// intermediate_path = edge.startNode, edge.endNode, hmapEndNode
						// Completing the triangle

						if (hmap.containsKey(hmapEndNode)) {
							List<Text> hmapStartNodes = hmap.get(hmapEndNode);

							for (Text hmapStartNode : hmapStartNodes) {
								if (hmapStartNode.equals(edgeStartNode)) {
									context.getCounter(CustomCounters.TRIANGLE_COUNTER).increment(1);
									outvalue.set(edgeStartNode.toString() + " " + edgeEndNode.toString() + " "
											+ hmapEndNode.toString() + " " + hmapStartNode.toString());
									context.write(edgeStartNode, outvalue);
								}
							}
						}

					}
				}
			}
		}
	}

	public enum CustomCounters {
		TRIANGLE_COUNTER
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Count Triangles");
		job.setJarByClass(RepJoin.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		// Delete output directory, only to ease local development; will not work on
		// AWS. ===========
		// final FileSystem fileSystem = FileSystem.get(conf);
		// if (fileSystem.exists(new Path(args[1]))) {
		// fileSystem.delete(new Path(args[1]), true);
		// }
		// ================
		job.setMapperClass(RepJoinMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		String max = args[2];
		job.getConfiguration().set("MAX", max);

		// Configure Distributed Cache
		// DistributedCache.addCacheFile(
		// new
		// Path("/app/sample_input/hw2-AkshaySyal-main/edges_3_nodes_complete_graph.csv").toUri(),
		// job.getConfiguration());
		// DistributedCache.setLocalFiles(job.getConfiguration(), args[0]);

		// job.addCacheFile(new
		// Path("/app/sample_input/hw2-AkshaySyal-main/edges_3_nodes_complete_graph.csv"));

		// Add the user files to the DistributedCache
		FileInputFormat.setInputDirRecursive(job, true);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.addCacheFile(new Path(args[0]).toUri());

		// return job.waitForCompletion(true) ? 0 : 1;
		if (!job.waitForCompletion(true)) {
			return 1;
		}

		// Triangle counter
		Counter c = job.getCounters().findCounter(CustomCounters.TRIANGLE_COUNTER);
		System.out.println(c.getValue() / 3);
		return 0;

	}

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			// MAX PARAM
			// Filter f = new Filter();
			// f.filter(62500);
			ToolRunner.run(new RepJoin(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}