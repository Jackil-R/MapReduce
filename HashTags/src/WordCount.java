
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import Format.IntIntPair;
public class WordCount {

  public static void runJob(String[] input, String output) throws Exception {

        Configuration conf = new Configuration();

    Job job = new Job(conf);
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TweetMapper.class);
    job.setReducerClass(TweetReducer.class);
    job.setCombinerClass(TweetCombiner.class);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.addCacheFile(new Path("/user/jr303/Country.txt").toUri());
    Path outputPath = new Path(output);
    FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
    FileOutputFormat.setOutputPath(job, outputPath);
    outputPath.getFileSystem(conf).delete(outputPath,true);
    job.waitForCompletion(true);
  }

  public static void main(String[] args) throws Exception {
       runJob(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);
  }

}

