import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class Netflix {
  private static void runJob1(String inputFilePath, String outputFilePath, String userID)
  {
    JobClient client = new JobClient();
    JobConf conf = new JobConf(Netflix.class);
    // specify output types
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    conf.set("userID", userID);

    // specify input and output dirs
    FileInputFormat.addInputPath(conf, new Path(inputFilePath));
    FileOutputFormat.setOutputPath(conf, new Path(outputFilePath));
    // specify a mapper
    conf.setMapperClass(NetflixUserMapper.class);
    // specify a combiner. For this one we can use the reducer code
    conf.setCombinerClass(NetflixUserReducer.class);
    // specify a reducer
    conf.setReducerClass(NetflixUserReducer.class);
    client.setConf(conf);
    try {
      JobClient.runJob(conf);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  private static void runJob2(String inputFilePath, String outputFilePath, String userDataPath, String userID)
  {
    JobClient client = new JobClient();
    JobConf conf = new JobConf(Netflix.class);
    // specify output types
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    conf.set("userDataFilePath", userDataPath);
    conf.set("userID", userID);

    // specify input and output dirs
    FileInputFormat.addInputPath(conf, new Path(inputFilePath));
    FileOutputFormat.setOutputPath(conf, new Path(outputFilePath));
    // specify a mapper
    conf.setMapperClass(NetflixUserMatcherMapper.class);
    // specify a combiner. For this one we can use the reducer code
    conf.setCombinerClass(NetflixUserMatcherReducer.class);
    // specify a reducer
    conf.setReducerClass(NetflixUserMatcherReducer.class);
    client.setConf(conf);
    try {
      JobClient.runJob(conf);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  private static void runJob3(String inputFilePath, String outputFilePath)
  {
    JobClient client = new JobClient();
    JobConf conf = new JobConf(Netflix.class);
    // specify output types
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    // specify input and output dirs
    FileInputFormat.addInputPath(conf, new Path(inputFilePath));
    FileOutputFormat.setOutputPath(conf, new Path(outputFilePath));
    // specify a mapper
    conf.setMapperClass(TopTenMapper.class);
    // specify a combiner. For this one we can use the reducer code
    conf.setCombinerClass(TopTenReducer.class);
    // specify a reducer
    conf.setReducerClass(TopTenReducer.class);
    conf.setNumReduceTasks(1);
    client.setConf(conf);
    try {
      JobClient.runJob(conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    JobClient client = new JobClient();
    JobConf conf = new JobConf(Netflix.class);

    String userID = args[4];
    String inputFile = args[0];
    String outputFile1 = args[1];
    String outputFile2 = args[2];
    String outputFile3 = args[3];

    runJob1(inputFile, outputFile1, userID);
    runJob2(inputFile, outputFile2, outputFile1 + "/part-00000", userID);
    runJob3(outputFile2 + "/part-00000", outputFile3);

  }
}
