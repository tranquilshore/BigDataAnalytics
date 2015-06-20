import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class MostUsedApp {
	public static void main(String args[]) throws IOException{
		JobConf conf = new JobConf(MostUsedApp.class);
		conf.setJobName("MostUsedApp");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setMapperClass(MostUsedAppMapper.class);
		conf.setReducerClass(MostUsedAppReducer.class);
		
		Path inp = new Path("hdfs://localhost:54310/data3");
		Path out = new Path("hdfs://localhost:54310/out3");
		
		FileInputFormat.addInputPath(conf, inp);
		FileOutputFormat.setOutputPath(conf, out);
		
		JobClient.runJob(conf);
	}
}
