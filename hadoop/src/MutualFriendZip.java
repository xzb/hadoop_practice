import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by xiezebin on 2/11/16.
 */
public class MutualFriendZip extends MutualFriend
{
    private final static String USAGE = "Usage: MutualFriendZip <in_adj> <in_data> <out> <user id,user id>";
    private final static String TEMP_FOLDER = "/zxx140430/output/temp";


    // in-memory join
    public static class MapForStep2
            extends Mapper<LongWritable, Text, Text, Text>
    {
        String obFriendPair;
        Set<String> obMutualUIDSet;


        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            // case 1: no mutual friend, just return    todo emit one empty record
            if (obMutualUIDSet.isEmpty())
            {
                return;
            }

            String[] loLineOfData = value.toString().split(",");
            String loCurrentUID = loLineOfData[0];
            // case 2: current user is not mutual friend, just return
            if (!obMutualUIDSet.contains(loCurrentUID))
            {
                return;
            }

            // emit data
            String loFriendName = loLineOfData[1] + " " + loLineOfData[2];
            String loValueOut = loLineOfData[6];        // zip
            context.write(new Text(obFriendPair), new Text(loFriendName + ":" + loValueOut));
        }

        protected void setup(Context context) throws IOException, InterruptedException
        {
            super.setup(context);

            Configuration conf = context.getConfiguration();
            String loPathString = conf.get(DRIVER_TO_MAPPER_KEY);
            Path loPath = new Path("hdfs://cshadoop1" + loPathString);  // Location of file in HDFS

            obMutualUIDSet = new HashSet<>();

            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fss = fs.listStatus(loPath);
            for (FileStatus status : fss)                      // read all files under path
            {
                Path pt = status.getPath();
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line = br.readLine();
                while (line != null)
                {
                    String[] loCacheDataSp = line.split("\\t");
                    if(loCacheDataSp.length > 1)
                    {
                        obFriendPair = loCacheDataSp[0];

                        // save mutual friend id
                        String[] loMutualUID = loCacheDataSp[1].split(",");
                        for (int i = 0; i < loMutualUID.length; i++)
                        {
                            obMutualUIDSet.add(loMutualUID[i]);
                        }
                    }
                    line=br.readLine();
                }
            }
        }
    }

    public static class ReduceForStep2
            extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException
        {
            StringBuilder loBuilder = new StringBuilder();
            for (Text lpMutualZip : values)
            {
                loBuilder.append(",");
                loBuilder.append(lpMutualZip);
            }
            if (loBuilder.length() > 0)
            {
                loBuilder.deleteCharAt(0);
            }

            context.write(new Text(key), new Text(loBuilder.toString()));
        }
    }


    // Driver program
    public static void main(String[] args) throws Exception
    {
        String[] otherArgs = getOtherArgs(args, 4, USAGE);

        // ==== EXEC first step =====================
        String loTargetUID = otherArgs[3];
        Job loJobStep1 = setupJob(Map.class, Reduce.class, otherArgs[0], TEMP_FOLDER, loTargetUID);

        // ==== EXEC second step ====================
        if (loJobStep1.waitForCompletion(true))
        {
            Job loJobStep2 = setupJob(MapForStep2.class, ReduceForStep2.class, otherArgs[1], otherArgs[2], TEMP_FOLDER);

            System.exit(loJobStep2.waitForCompletion(true) ? 0 : 1);
        }
        else
        {
            System.exit(1);
        }

    }
}
