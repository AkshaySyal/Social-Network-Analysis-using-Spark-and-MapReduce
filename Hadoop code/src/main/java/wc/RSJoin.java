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

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
// import wc.WordCount.TokenizerMapper;

// Job names
// Create_Path2
// Count_Triangles

// 3 Mappers: 
// Mapper_Create_Path2: input edges for job_create_path2
// Mapper_Path2_Count_Triangles: input path2 for job_count_triangles
// Mapper_Edges_Count_Triangles: input edges for job_count_triangles

// 2 Reducers:
// Reducer_Create_Path2
// Reducer_Count_Triangles

public class RSJoin extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(RSJoin.class);

	public static class MapperCreatePath2 extends Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] nodes = line.split(","); // Split the CSV line

			int max = Integer.parseInt(context.getConfiguration().get("MAX"));

			String followerNode = nodes[0];
			String followedNode = nodes[1];

			int followedNodeId = Integer.parseInt(followedNode);
			int followerNodeId = Integer.parseInt(followerNode);

			if (followedNodeId < max && followerNodeId < max) {
				outkey.set(followerNode);
				outvalue.set("E" + followedNode);
				context.write(outkey, outvalue);

				outkey.set(followedNode);
				outvalue.set("S" + followerNode);
				context.write(outkey, outvalue);
			}

		}
	}

	public static class MapperPath2CountTriangles extends Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			System.out.println("from MapperPath2CountTriangles " + line);

			String[] nodes = line.split(" ");

			String garbage = nodes[0];
			String start = nodes[1];
			String mid = nodes[2];
			String end = nodes[3];

			// DONT allow paths that are loops i.e. 2,1,2 so start==end
			if (!start.equals(end)) {
				outkey.set(end);
				outvalue.set("p" + start + " " + mid);
				context.write(outkey, outvalue);
			}

		}
	}

	public static class MapperEdgesCountTriangles extends Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			System.out.println("from MapperEdgesCountTriangles " + line);
			String[] nodes = line.split(","); // Split the CSV line

			String followerNode = nodes[0];
			String followedNode = nodes[1];

			outkey.set(followerNode);
			outvalue.set("e" + followedNode);
			context.write(outkey, outvalue);
		}
	}

	public static class ReducerCreatePath2 extends Reducer<Text, Text, Text, Text> {
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
					// context.getCounter(CustomCounters.TWO_PATH_COUNTER).increment(1);
					outvalue.set(startNode.toString() + " " + startNode.toString() + " " + midNode.toString() + " "
							+ endNode.toString());
					context.write(startNode, outvalue);
				}
			}

		}
	}

	public static class ReducerCountTriangles extends Reducer<Text, Text, Text, Text> {
		private ArrayList<Text> pathList = new ArrayList<Text>();
		private ArrayList<Text> nodeList = new ArrayList<Text>();
		private Text triangle = new Text();

		@Override
		public void reduce(final Text node, final Iterable<Text> records, final Context context)
				throws IOException, InterruptedException {

			pathList.clear();
			nodeList.clear();

			for (Text rec : records) {
				if (rec.charAt(0) == 'e') {
					nodeList.add(new Text(rec.toString().substring(1)));
				} else if (rec.charAt(0) == 'p') {
					pathList.add(new Text(rec.toString().substring(1)));
				}
			}

			// System.out.println("printing pathlist");
			// for (Text path : pathList) {
			// System.out.println(path);
			// }

			// System.out.println("printing edgelist");
			// for (Text endNode : nodeList) {
			// System.out.println(endNode);
			// }
			// p.charAt(0) == n.charAt(0)
			for (Text n : nodeList) {
				for (Text p : pathList) {
					String nStr = n.toString();
					String[] pList = p.toString().split(" ");
					String pStr = pList[0];

					System.out.println(nStr);
					System.out.println(pStr);

					if (nStr.equals(pStr)) {
						context.getCounter(CustomCounters.TRIANGLE_COUNTER).increment(1);
						triangle.set(p.toString() + " " + node.toString() + " " + n.toString());
						context.write(node, triangle);
					}
				}
			}

		}
	}

	// Global counter for product sum
	public enum CustomCounters {
		TRIANGLE_COUNTER
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job createPath2 = Job.getInstance(conf, "Create Path2");
		createPath2.setJarByClass(RSJoin.class);
		final Configuration jobConf = createPath2.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		// Delete output directory, only to ease local development; will not work on
		// AWS. ===========
		// final FileSystem fileSystem = FileSystem.get(conf);
		// if (fileSystem.exists(new Path(args[1]))) {
		// fileSystem.delete(new Path(args[1]), true);
		// }
		// ================

		// createPath2 job mapper
		createPath2.setMapperClass(MapperCreatePath2.class);

		// createPath2 job reducer
		createPath2.setReducerClass(ReducerCreatePath2.class);

		createPath2.setOutputKeyClass(Text.class);
		createPath2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(createPath2, new Path(args[0]));
		FileOutputFormat.setOutputPath(createPath2, new Path(args[1]));
		String max = args[2];
		createPath2.getConfiguration().set("MAX", max);

		if (!createPath2.waitForCompletion(true)) {
			return 1;
		}

		// Job 2: Count_Triangles
		final Configuration conf2 = getConf();
		final Job countTriangles = Job.getInstance(conf2, "Count Triangles");
		countTriangles.setJarByClass(RSJoin.class);

		// countTriangles mappers (2 mappers)

		// MapperPath2CountTriangles
		MultipleInputs.addInputPath(countTriangles, new Path(args[1]),
				TextInputFormat.class, MapperPath2CountTriangles.class);

		// MapperEdgesCountTriangles
		MultipleInputs.addInputPath(countTriangles, new Path(args[0]),
				TextInputFormat.class, MapperEdgesCountTriangles.class);

		// createPath2 job reducer
		countTriangles.setReducerClass(ReducerCountTriangles.class);

		FileOutputFormat.setOutputPath(countTriangles, new Path("trianglesOutput"));

		countTriangles.setOutputKeyClass(Text.class);
		countTriangles.setOutputValueClass(Text.class);

		// System.exit(job.waitForCompletion(true) ? 0 : 3);

		if (!countTriangles.waitForCompletion(true)) {
			return 1;
		}

		// Triangle counter
		Counter c = countTriangles.getCounters().findCounter(CustomCounters.TRIANGLE_COUNTER);
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
			ToolRunner.run(new RSJoin(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}