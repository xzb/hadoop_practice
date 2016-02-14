import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by xiezebin on 2/13/16.
 */
public class TopAverageAge extends MapReduceBase
{
    private final static String USAGE = "Usage: TopAverageAge <in_adj> <in_data> <out> <user id,user id>";
    private final static String TEMP_FOLDER = "/zxx140430/output/temp";

    public static class MapForStep2
            extends Mapper<LongWritable, Text, Text, Text>
    {

    }

    public static class ReduceForStep2
            extends Reducer<Text, Text, Text, Text>
    {

    }

    // Driver program
    public static void main(String[] args) throws Exception
    {
        String[] otherArgs = getOtherArgs(args, 4, USAGE);

        // ==== EXEC first step =====================
        String loTargetUID = otherArgs[3];
//        Job loJobStep1 = setupJob(MutualFriend.Map.class, MutualFriend.Reduce.class, otherArgs[0], TEMP_FOLDER, loTargetUID);

        // ==== EXEC second step ====================
//        if (loJobStep1.waitForCompletion(true))
//        {
//            Job loJobStep2 = setupJob(MapForStep2.class, ReduceForStep2.class, otherArgs[1], otherArgs[2], TEMP_FOLDER);
//
//            System.exit(loJobStep2.waitForCompletion(true) ? 0 : 1);
//        }
//        else
//        {
//            System.exit(1);
//        }

    }
}
