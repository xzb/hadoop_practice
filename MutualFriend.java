import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

/**
 * Created by xiezebin on 2/11/16.
 */
public class MutualFriend
{
    private final static String sTargetUIDKey = "UID_KEY";

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text>
    {
        Set<String> obTargetUIDSet;

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            // user don't have friend, do nothing
            String[] loLineOfData = value.toString().split("\\t");
            if (loLineOfData.length < 2)
            {
                return;
            }

            // prepare target user id
            String loTargetUIDs = context.getConfiguration().get(sTargetUIDKey);
            obTargetUIDSet = new HashSet<>();
            String[] loTargetUIDsp = loTargetUIDs.split(",");
            for (int i = 0; i < loTargetUIDsp.length; i++)
            {
                obTargetUIDSet.add(loTargetUIDsp[i]);
            }

            // current user is not target user, do nothing
            String loCurrentUID = loLineOfData[0];
            if (!obTargetUIDSet.contains(loCurrentUID))
            {
                return;
            }

            // emit data
            String loKeyOut = loTargetUIDsp[0] + "," + loTargetUIDsp[1];
            context.write(new Text(loKeyOut), new Text(loLineOfData[1]));
        }
    }


    public static class Reduce
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {

            Iterator<Text> loIterator = values.iterator();
            StringBuilder loBuilder = new StringBuilder();

            if (loIterator.hasNext())
            {
                String[] loFriendID = loIterator.next().toString().split(",");       // list of friends
                Set<String> loFirstFriendSet = new HashSet<>();
                for (int i = 0; i < loFriendID.length; i++)
                {
                    loFirstFriendSet.add(loFriendID[i]);
                }

                if (loIterator.hasNext())
                {
                    String[] loSecFriendID = loIterator.next().toString().split(",");       // list of friends
                    for (int i = 0; i < loSecFriendID.length; i++)
                    {
                        if (loFirstFriendSet.contains(loSecFriendID[i]))
                        {
                            loBuilder.append(",");
                            loBuilder.append(loSecFriendID[i]);
                        }
                    }
                    if (loBuilder.length() > 0)
                    {
                        loBuilder.deleteCharAt(0);
                    }
                }
            }

            context.write(key, new Text(loBuilder.toString()));
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 3) {
            System.err.println("Usage: MutualFriend <in> <out> <user id,user id>");
            System.exit(2);
        }

        // save target user id
        String loTargetUIDs = otherArgs[2];
        String[] loTargetUIDsp = loTargetUIDs.split(",");
        if (loTargetUIDsp.length != 2)
        {
            System.err.println("Usage: MutualFriend <in> <out> <user id,user id>");
            System.exit(2);
        }

        conf.set(sTargetUIDKey, loTargetUIDs);

        // create a job with name "wordcount"
        Job job = new Job(conf, "MutualFriend");
        job.setJarByClass(MutualFriend.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);


        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
