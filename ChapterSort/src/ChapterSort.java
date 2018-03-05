import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;
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
		private static Text currentChapter = null;
		static final Pattern p = Pattern.compile("(chapter \\S+)\\.", Pattern.CASE_INSENSITIVE);
	    private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
			Matcher m = p.matcher(value.toString());
			if(m.find())
			{
				currentChapter = new Text(m.group(1));
			}
			
			if(currentChapter == null)
				return;
			
		      StringTokenizer itr = new StringTokenizer(value.toString());
		    while (itr.hasMoreTokens()) {
		    	word.set(itr.nextToken());
		    	context.write(currentChapter, word);
		    }
		}
	}
	
	public static class ChapterSorter extends Reducer<Text, Text, Text, Text>
	{
		public static Map<String, Map<String, Integer>> chapterMap = new HashMap<>();
		
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
			chapterMap.put(key.toString(), new HashMap<String, Integer>());

			for(String word: words.keySet())
			{
				if(words.get(word) >= Integer.parseInt(context.getConfiguration().get("threshhold")))
				{
					chapterMap.get(key.toString()).put(word, words.get(word));
				}
			}
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			Map<String, Integer> keywordMap = new HashMap<>();
			for(String chapter : chapterMap.keySet())
			{
				context.write(new Text(chapter), new Text("contains"));
				for(String word : chapterMap.get(chapter).keySet())
				{
					if(!keywordMap.containsKey(word))
					{
						keywordMap.put(word, 0);
					}
					keywordMap.put(word, keywordMap.get(word) + chapterMap.get(chapter).get(word));
				}
			}

			SortedMap<String, Integer> m = new TreeMap(keywordMap);
			for(Entry<String, Integer> entry : m.entrySet()) {
				context.write(new Text(entry.getKey()), new Text("" + entry.getValue()));									
				for(String chapter : chapterMap.keySet())
				{
					if(chapterMap.get(chapter).containsKey(entry.getKey()))
						context.write(new Text(chapter), new Text("" + chapterMap.get(chapter).get(entry.getKey())));									
				}
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception{
	    Configuration conf = new Configuration();
	    conf.set("threshhold", args[2]);
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
