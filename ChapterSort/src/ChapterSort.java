import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
		static final Pattern p = Pattern.compile("(chapter \\S+)\\.", Pattern.CASE_INSENSITIVE);
		
		@Override
		public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
			Matcher m = p.matcher(value.toString());
			if(m.find())
			{
				currentChapter = m.group(1);
			}
			
			if(currentChapter != null)
				context.write(new Text(currentChapter), new Text(value.toString()));

		}
	}
	
	public static class ChapterSorter extends Reducer<Text, Text, Text, Text>
	{
		public static Map<String, Map<String, Integer>> chapterList = new HashMap<>();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, 
                Context context
                ) throws IOException, InterruptedException {
			Map<String, Integer> words = new HashMap<>();
			for(Text val : values)
			{
				String word = val.toString();
				if(!words.containsKey(word))
					words.put(word, 0);
				
				words.put(word, words.get(word)+1);
			}
			chapterList.put(key.toString(), new HashMap<String, Integer>());
			for(String word: words.keySet())
			{
				if(words.get(word) >= 10)
				{
					chapterList.get(key.toString()).put(word, words.get(word));
				}
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			for(String chapter : chapterList.keySet())
			{
				context.write(new Text(chapter), new Text("contains"));
				for(String word : chapterList.get(chapter).keySet())
				{
					context.write(new Text(word), new Text("" + chapterList.get(chapter).get(word)));					
				}
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
