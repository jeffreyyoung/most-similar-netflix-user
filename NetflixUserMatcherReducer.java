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
    private Text highestUserID;
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
        if (sum > 5)
        {
          // this.highestUserID = key;
          // this.highestSum = sum;
          output.collect(key, new IntWritable(sum));
        }
        //output.collect(key, new IntWritable(sum));
    }

    // @Override
    // public void close() throws IOException
    // {
    //   output.collect(highestUserID, new IntWritable(highestSum));
    // }
}
