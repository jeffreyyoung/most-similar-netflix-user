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

public class TopTenMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private TreeMap<Integer, Text> topTen = new TreeMap<Integer, Text>();
    private OutputCollector<Text, IntWritable> outputer;

    @Override
    public void map(LongWritable key, Text value,
                    OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        this.outputer = output;
        String line = value.toString();
        String values[] = line.split("\\s+");
        if (values.length >= 2)
        {
            String userID = values[0];
            Integer matches = Integer.parseInt(values[1]);
            topTen.put(matches, userID);

            if (topTen.size() > 10)
            {
                topTen.remove(topTen.firstKey());
            }
        }
    }

    @Override
    public void close() throws IOException {

        for(Map.Entry<Integer,Text> entry : topTen.entrySet()) {
            Integer count = entry.getKey();
            Text userID = entry.getValue();
            outputer.collect(userID, count);
        }
    }
}
