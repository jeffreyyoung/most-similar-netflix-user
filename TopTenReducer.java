import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TopTenReducer extends MapReduceBase
        implements Reducer<Text, IntWritable, Text, IntWritable> {

    private TreeMap<Integer, Text> topTen = new TreeMap<Integer, Text>();
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

        topTen.put(new IntWritable(sum), key);

        if (topTen.size() > 10)
        {
            topTen.remove(topTen.firstKey());
        }
    }

    @Override
    public void close() throws IOException {

        for(Map.Entry<Integer,Text> entry : topTen.entrySet()) {
            Integer count = entry.getKey();
            Text userID = entry.getValue();
            outputer.collect(userID, new IntWritable(count));
        }
    }
}
