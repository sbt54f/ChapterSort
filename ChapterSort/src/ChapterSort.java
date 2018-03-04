import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Reducer;

public class ChapterSort {

	public static class ChapterMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		// init to "chapter 0"
		static String currentChapter = null;
		static final Pattern p = Pattern.compile("chapter (\\S+)\\.", Pattern.CASE_INSENSITIVE);
		
		@Override
		public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
			Matcher m = p.matcher(value.toString());
			if(m.matches())
			{
				currentChapter = m.group(0);
				context.write(new Text("YES"), new Text(currentChapter));
			}else
			{
				context.write(new Text("NO"), new Text(value.toString()));
			}
				
//			StringTokenizer itr = new StringTokenizer(value.toString());
//			while (itr.hasMoreTokens()) {
//        		word.set(itr.nextToken());
//        		context.write(word, one);
//		  	}
//			context.write(value, new Text("eyyy"));
		}
	}
	
	public static class ChapterSorter extends Reducer<Text, Text, Text, Text>
	{
		@Override
		public void reduce(Text key, Iterable<Text> values, 
                Context context
                ) throws IOException, InterruptedException {
			if(key.toString().equals("YES"))
			{

				for (Text val : values) {
					context.write(new Text(key.toString()), new Text(val.toString()));
				}
			}else
			{
				int sum = 0;
				for (Text val : values) {
					sum++;
				}
				context.write(new Text("NO"), new Text(""+sum));
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception{
	    Configuration conf = new Configuration();
	    Job job = new Job(conf, "chapter sort");
	    job.setJarByClass(ChapterSort.class);
	    job.setMapperClass(ChapterMapper.class);
	    job.setReducerClass(ChapterSorter.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
