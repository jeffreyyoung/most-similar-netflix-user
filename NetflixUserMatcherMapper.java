import java.io.IOException;
import java.io.PrintWriter;
import java.util.StringTokenizer;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

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

public class NetflixUserMatcherMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Map<String, String> userData;
    private String userDataPath;
    private String userID;

    @Override
    public void configure(JobConf job)
    {
        this.userDataPath = (String) job.get("userDataFilePath");
        this.userID = (String) job.get("userID");
        try {
            this.userData = getUserData(userDataPath);
        } catch (Exception e) {
        //
        }
    }

    private static Map<String, String> getUserData(String fileName) throws Exception{
        Scanner scanner;
        try {
        scanner = new Scanner(new FileReader(fileName));
      } catch (Exception e) {
        throw new Exception("can't find the file...");
      }
        HashMap<String, String> userData = new HashMap<String, String>();

        while (scanner.hasNextLine()) {
            String[] columns = scanner.nextLine().split("\\s+");
            if (columns.length >= 2)
                userData.put(columns[0].trim(), columns[1].trim());
        }
        return userData;
    }

    private boolean shouldEmitUser(String entryUserID, String showID, Integer stars)
    {
      return (!entryUserID.trim().equals(userID.trim())
              && userData.containsKey(showID.trim())
              && Integer.parseInt(userData.get(showID.trim())) == stars);
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
            if (shouldEmitUser(entryUserID, showID, stars))
            {
                word.set(entryUserID);
                output.collect(word, one);
            }
        }
    }
}
