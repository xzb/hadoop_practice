import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by xiezebin on 2/13/16.
 */
public class MapReduceBase
{
    protected final static String DRIVER_TO_MAPPER_KEY = "DRIVER_TO_MAPPER_KEY";

    protected static String[] getOtherArgs(String[] args, int numOfOtherArgs, String arUsage) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != numOfOtherArgs) {
            System.err.println(arUsage);
            System.exit(2);
        }

        // save target user id
        String loTargetUIDs = otherArgs[numOfOtherArgs - 1];
        String[] loTargetUIDsp = loTargetUIDs.split(",");
        if (loTargetUIDsp.length != 2)
        {
            System.err.println(arUsage);
            System.exit(2);
        }

        return otherArgs;
    }

    protected static Job setupJob(
            Class<? extends Mapper> arMapClass,
            Class<? extends Reducer> arReduceClass,
            String inputPath,
            String outputPath,
            String arDriverToMapperVal)
            throws Exception
    {
        Configuration conf = new Configuration();
        // set argument
        String loTargetUIDs = arDriverToMapperVal;
        conf.set(DRIVER_TO_MAPPER_KEY, loTargetUIDs);

        // reuse output folder
        FileSystem loFS = FileSystem.get(new Configuration());
        loFS.delete(new Path(outputPath), true);


        // create a job with name CLASS_NAME
        Job job = new Job(conf, "JOB");
        job.setJarByClass(MutualFriend.class);
        job.setMapperClass(arMapClass);
        job.setReducerClass(arReduceClass);

        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(inputPath));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        //Wait till job completion
        return job;
    }
}
