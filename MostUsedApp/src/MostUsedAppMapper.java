import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MostUsedAppMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable>{
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException{
		String strline = value.toString();
		if((!(strline.isEmpty()) && (strline.length()>68 && strline.charAt(48)!='%')) && (strline.charAt(45)=='S'|| strline.charAt(45)=='R')){
			int len =  strline.length();
			String appname = strline.substring(68, len);
			int i=46;
			while(strline.charAt(i)==' ')
				i++;
			double cpusage = Double.parseDouble(strline.substring(i, 52));
			output.collect(new Text(appname),new DoubleWritable(cpusage));
		}
	}
}
