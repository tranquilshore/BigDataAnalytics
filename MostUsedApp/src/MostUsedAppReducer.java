import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class MostUsedAppReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException{
		double sum = 0;
		while(values.hasNext()){
			sum += values.next().get();
		}
		output.collect(key, new DoubleWritable(sum));
	}
}
