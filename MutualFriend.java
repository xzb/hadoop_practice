import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by xiezebin on 2/11/16.
 */
public class MutualFriend extends MapReduceBase
{
    private final static String USAGE = "Usage: MutualFriend <in> <out> <user id,user id>";

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text>
    {
        Set<String> obTargetUIDSet;

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            String[] loLineOfData = value.toString().split("\\t");

            // prepare target user id
            String loTargetUIDs = context.getConfiguration().get(DRIVER_TO_MAPPER_KEY);
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
            String loValueOut = loLineOfData.length < 2 ? "" : loLineOfData[1];     // if user don't have friend, emit empty value
            context.write(new Text(loKeyOut), new Text(loValueOut));
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
        String[] otherArgs = getOtherArgs(args, 3, USAGE);
        Job loJob = setupJob(Map.class, Reduce.class, otherArgs[0], otherArgs[1], otherArgs[2]);

        System.exit(loJob.waitForCompletion(true) ? 0 : 1);
    }

}
