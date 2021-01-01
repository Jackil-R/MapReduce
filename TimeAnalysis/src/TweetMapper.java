import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import Format.IntIntPair;
import org.apache.commons.lang.StringUtils;

public class TweetMapper extends Mapper<Object, Text, Text, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	// Format per tweet is id;date;hashtags;tweet;
    	String dump = value.toString();
    	if(StringUtils.ordinalIndexOf(dump,";",4)>-1){
        	int startIndex = StringUtils.ordinalIndexOf(dump,";",1) + 1;
		int endIndex = StringUtils.ordinalIndexOf(dump,",",1)+1;
        	String tweet = dump.substring(startIndex,endIndex).toLowerCase();

		if(tweet.length()<=140 && tweet.length()>0){
			context.write(new Text(tweet),one);
		}
	}
        
    }
}
