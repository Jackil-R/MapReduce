import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import Format.IntIntPair;
import org.apache.commons.lang.StringUtils;
import java.util.Arrays;


public class TweetMapper extends Mapper<Object, Text, Text, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    String tweet;
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	// Format per tweet is id;date;hashtags;tweet;
    	if(value.toString().isEmpty() == false){
		ArrayList<String> country_Name = new ArrayList<String>();
		ArrayList<String> three_Code = new ArrayList<String>();
		ArrayList<String> two_Code = new ArrayList<String>();
		
		BufferedReader in = new BufferedReader(new FileReader("Country.txt"));
	       	
		String line="";
		
		while((line = in.readLine()) != null){
			line=line.replaceAll("\\s","");			
			country_Name.add(line);
			if((line = in.readLine()) !=null){
				line=line.replaceAll("\\s","");	
				three_Code.add(line);
			}if((line = in.readLine()) !=null){
				line=line.replaceAll("\\s","");	
				two_Code.add(line);
			}
		}
		in.close();
		
		String dump = value.toString();
		if(StringUtils.ordinalIndexOf(dump,";",4)>-1){
		    int startIndex = StringUtils.ordinalIndexOf(dump,";",2) + 1;
		    int endIndex = StringUtils.ordinalIndexOf(dump,";",3);
		    hashtags = dump.substring(startIndex,endIndex).toLowerCase();
		} 
		
		int index=0;

		String[] tags = hashtags.split(" ");

		for(int i = 0; i<tags.length;i++){

			if(tags[i].startsWith("team")){
				String country = tags[i].substring(StringUtils.ordinalIndexOf(tags[i],"m",1) + 1,tags[i].length());
				if(country_Name.contains(country) && country.length() > 3){
					index=country_Name.indexOf(country);
					context.write( new Text(country_Name.get(index)), one);
				}
				else if(three_Code.contains(country) && country.length() == 3){
 					index=three_Code.indexOf(country);
					context.write( new Text(country_Name.get(index)), one);
				}
				else if(two_Code.contains(country) && country.length() == 2){
					index=two_Code.indexOf(country);
					context.write( new Text(country_Name.get(index)), one);
				}
		
				 

	
			}else if (tags[i].startsWith("go")){
				String country = tags[i].substring(StringUtils.ordinalIndexOf(tags[i],"o",1) + 1,tags[i].length());
				if(country_Name.contains(country) && country.length() > 3){
					index=country_Name.indexOf(country);
					context.write( new Text(country_Name.get(index)), one);
				}
				else if(three_Code.contains(country) && country.length() == 3){
 					index=three_Code.indexOf(country);
					context.write( new Text(country_Name.get(index)), one);
				}
				else if(two_Code.contains(country) && country.length() == 2){
					index=two_Code.indexOf(country);
					context.write( new Text(country_Name.get(index)), one);
				}

			
			}

		}
        }
    }
}
