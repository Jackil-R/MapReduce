import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import Format.IntIntPair;

public class TweetReducer extends Reducer<IntIntPair, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(IntIntPair key, Iterable<IntWritable> values, Context context)

              throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value : values) {
            sum+=value.get();
        }

        result.set(sum);
       	context.write(new Text(key.getFirst().toString()+" - "+ key.getSecond().toString()),result);

    }

}
