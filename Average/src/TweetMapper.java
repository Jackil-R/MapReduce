import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import Format.IntIntPair;
import org.apache.commons.lang.StringUtils;

public class TweetMapper extends Mapper<Object, Text, IntWritable, IntIntPair> { 
    private final IntWritable one = new IntWritable(1);
    private final IntWritable length = new IntWritable(1);
    private IntIntPair pair = new IntIntPair(0,0);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	// Format per tweet is id;date;hashtags;tweet;
    	String dump = value.toString();
    	if(StringUtils.ordinalIndexOf(dump,";",4)>-1){
        	int startIndex = StringUtils.ordinalIndexOf(dump,";",3) + 1;
        	//String tweet = dump.substring(startIndex,dump.lastIndexOf(';')).replaceAll("[a-zA-Z0-9\\s]", " ");
		String tweet = dump.substring(startIndex,dump.lastIndexOf(';'));
		length.set(tweet.length());
		pair.set(length,one);
		context.write(one,pair);
	}
        
    }
}
