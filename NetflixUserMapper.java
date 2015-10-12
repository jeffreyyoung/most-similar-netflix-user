import java.io.IOException;
import java.io.PrintWriter;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

public class NetflixUserMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, IntWritable> {

  private final IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private String userID = "";

  @Override
  public void configure(JobConf job)
  {
    this.userID = (String) job.get("userID");
  }

  @Override
  public void map(LongWritable key, Text value,
      OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

    String line = value.toString();
    String values[] = line.split("\\s+");
    if (values.length >= 3)
    {
      String entryUserID = values[0];
      Integer stars = Integer.parseInt(values[2]);
      String showID = values[1];
      if (entryUserID.equals(userID) && stars >= 3)
      {
        word.set(showID);
        output.collect(word, new IntWritable(stars));
      }
    }
  }
}
