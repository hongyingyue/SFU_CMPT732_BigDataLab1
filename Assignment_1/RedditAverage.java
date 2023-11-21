import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.json.JSONObject;


public class RedditAverage extends Configured implements Tool {

	public static class RedditMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{
        private Text red_name = new Text();
		private LongPairWritable red_pair = new LongPairWritable();

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
            JSONObject record = new JSONObject(value.toString());
            red_name.set((String) record.get("subreddit"));
			Integer intScoreValue = (Integer) record.get("score");
			Long longScoreValue = intScoreValue.longValue();
            red_pair.set(1, longScoreValue);
            context.write(red_name, red_pair);
		}
	}

	public static class AverageReducer
	extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
		private DoubleWritable outputValue = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			long sum_0 = 0;
			long sum_1 = 0;
			for (LongPairWritable val: values) {
				sum_0 += val.get_0();
				sum_1 += val.get_1();
			}
			double averagedValue = (double) sum_1/sum_0;
			outputValue.set(averagedValue);
			context.write(key, outputValue);
		}
	}

	public static class SumCombiner
	extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
		private LongPairWritable result = new LongPairWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			long sum_0 = 0;
			long sum_1 = 0;
			for (LongPairWritable val: values) {
				sum_0 += val.get_0();
				sum_1 += val.get_1();
			}
			result.set(sum_0, sum_1);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "reddit average");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(RedditMapper.class);
		job.setCombinerClass(SumCombiner.class);
		job.setReducerClass(AverageReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongPairWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
