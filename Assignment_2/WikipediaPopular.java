import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WikipediaPopular extends Configured implements Tool {

	public static class HourMapper
	extends Mapper<LongWritable, Text, Text, LongWritable>{
		private Text hourSpan = new Text();
        private LongWritable views = new LongWritable();

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
            String line = value.toString();
			String[] itr = line.split(" ");
            String time = itr[0];
            String lang = itr[1];
            String title = itr[2];
            Long request = Long.parseLong(itr[3]);
            if (lang.equals("en") && !title.equals("Main_Page") && !title.startsWith("Special:")) {
                hourSpan.set(time);
                views.set(request);
                context.write(hourSpan, views);
            }
		}
	}

	public static class MaxReducer
	extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		@Override
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context
				) throws IOException, InterruptedException {
			Long max =(long) 0;
			for (LongWritable val : values) {
                if (val.get() > max) {
                    max = val.get();
                }
			}
			result.set(max);
			context.write(key, result);
		}
	}


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(HourMapper.class);
		job.setCombinerClass(MaxReducer.class);
		job.setReducerClass(MaxReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
