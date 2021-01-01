import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import Format.IntIntPair;

public class TweetReducer extends Reducer<IntWritable, IntIntPair, IntWritable, Text> {

    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntIntPair> values, Context context)

              throws IOException, InterruptedException {

        int sum = 0;
	int count = 0;

        for (IntIntPair value : values) {

            sum+=value.getFirst().get();
            count+=1;

        }
	
	double avg = sum/count;

        //result.set(sum);

       	context.write(key,new Text("Sum: " + sum + " Average: " + avg + " Count: " + count));

    }

}
