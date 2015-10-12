import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class NetflixUserMatcherReducer extends MapReduceBase
        implements Reducer<Text, IntWritable, Text, IntWritable> {

    private int highestSum = 0;
    private String highestUserID = "";
    private OutputCollector output;

    @Override
    public void reduce(Text key, Iterator values,
                       OutputCollector output, Reporter reporter) throws IOException {

        this.output = output;
        int sum = 0;
        while (values.hasNext()) {
            IntWritable value = (IntWritable) values.next();
            sum += value.get(); // process value
        }
        if (sum > highestSum)
        {
          highestUserID = key.get();
          highestSum = sum;
        }
        //output.collect(key, new IntWritable(sum));
    }

    @Override
    public void close()
    {
      output.collect(new Text(highestUserID), new IntWritable(highestSum));
    }
}
