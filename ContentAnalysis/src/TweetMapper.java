import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import Format.IntIntPair;
import org.apache.commons.lang.StringUtils;

public class TweetMapper extends Mapper<Object, Text, IntIntPair, IntWritable> { 
    private final IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	// Format per tweet is id;date;hashtags;tweet;
    	String dump = value.toString();
    	if(StringUtils.ordinalIndexOf(dump,";",4)>-1){
        	int startIndex = StringUtils.ordinalIndexOf(dump,";",3) + 1;
        	String tweet = dump.substring(startIndex,dump.lastIndexOf(';')).replaceAll("[a-zA-Z0-9\\s]", " ");
		
		int length = tweet.length();

		IntIntPair pairs = getPairs(length);
		
		if(tweet.length()<=140 && tweet.length()>0){
			context.write(pairs,one);
		}
	}
        
    }

    public IntIntPair getPairs(int length){
	int first=0;
	int second=0;

	if(length>0){
		if(length%5==0){
			first=length-4;
			second=first+4;
		}else{
			first=(length-(length%5))+1;
			second=first+4;
		}
	}
	
	return new IntIntPair(first,second);

   }
}
