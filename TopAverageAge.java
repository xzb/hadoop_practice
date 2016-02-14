import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by xiezebin on 2/13/16.
 */
public class TopAverageAge extends MapReduceBase
{
    private final static String USAGE = "Usage: TopAverageAge <in_adj> <in_data> <out>";
    private final static String TEMP_FOLDER = "/zxx140430/output/temp/1";
    private final static String TEMP_FOLDER_ALTER = "/zxx140430/output/temp/2";

    private final static String AHEAD_KEY = "0";
    private final static String BEHIND_KEY = "1";

    private final static String CURRENT_YEAR = "2016";
    private final static String AGE_TAG = "AGE_TAG";
    private final static String ADDRESS_TAG = "ADDRESS_TAG";

    // STEP 1: Join two tables by key of each friend in list
    private static class JoinFriendMapper
            extends Mapper<LongWritable, Text, TextPair, TextPair>
    {
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            String[] loLineOfData = value.toString().split("\\t");
            if (loLineOfData.length < 2)
            {
                return;
            }

            String loCurrentUID = loLineOfData[0];
            String[] loFriend = loLineOfData[1].split(",");
            for (int i = 0; i < loFriend.length; i++)
            {
                context.write(new TextPair(loFriend[i], BEHIND_KEY), new TextPair(BEHIND_KEY, loCurrentUID));
            }
        }
    }
    private static class JoinAgeMapper
            extends Mapper<LongWritable, Text, TextPair, TextPair>
    {
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            String[] loLineOfData = value.toString().split(",");

            String loCurrentUID = loLineOfData[0];
            String loDateOfBirth = loLineOfData[9];
            String loAddress = loLineOfData[3] + ", " + loLineOfData[4] + ", " + loLineOfData[5];
            Integer loAge = Integer.valueOf(CURRENT_YEAR) - Integer.valueOf(loDateOfBirth.split("/")[2]);

            context.write(new TextPair(loCurrentUID, AHEAD_KEY),
                    new TextPair(ADDRESS_TAG, loAddress));
            context.write(new TextPair(loCurrentUID, AHEAD_KEY),
                    new TextPair(AGE_TAG, "" + loAge));
        }
    }
    private static class JoinFriendAgeReducer
            extends Reducer<TextPair, TextPair, Text, TextPair>
    {
        public void reduce(TextPair key, Iterable<TextPair> values, Context context
        ) throws IOException, InterruptedException
        {
            String loCurrentUID = key.getFirst().toString();
            int loCount = 0;
            String loAge = "0";

            for (TextPair lpPair : values)
            {
                String loTag = lpPair.obFirstText.toString();
                String loValue = lpPair.obSecondText.toString();
                if (loCount < 2)                                    // ahead_key and behind_key group into one iterator
                {
                    if (loTag.equals(AGE_TAG))
                    {
                        // save age of current user
                        loAge = loValue;
                    }
                    else if (loTag.equals(ADDRESS_TAG))
                    {
                        // pass address of current user for future use
                        context.write(new Text(loCurrentUID), new TextPair(ADDRESS_TAG, loValue));
                    }
                    loCount++;
                }
                else
                {
                    // loValue is friend id, assign current user's age to each friend
                    context.write(new Text(loValue), new TextPair(AGE_TAG, loAge));
                }
            }
        }
    }

    // STEP 2: Calculate average age of friends of each user
    private static class CalculateAveAgeMapper
            extends Mapper<LongWritable, Text, Text, Text>
    {
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            String[] loLineOfData = value.toString().split("\\t");
            String loCurrentUID = loLineOfData[0];
            String loTagAndContent = loLineOfData[1];

            context.write(new Text(loCurrentUID), new Text(loTagAndContent));
        }
    }
    private static class CalculateAveAgeReducer
            extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException
        {
            String loAddress = "";
            int loAgeSum = 0;
            int loCount = 0;

            for (Text loText : values)
            {
                String[] loData = loText.toString().split(":");
                String loTag = loData[0];
                String loContent = loData[1];

                if (loTag.equals(ADDRESS_TAG))
                {
                    loAddress = loContent;
//                    context.write(key, loText);       //do not emit second line
                }
                else
                {
                    loAgeSum += Integer.valueOf(loContent);
                    loCount++;
                }
            }
            double loAveAge = loAgeSum * 1.0 / loCount;
            String loOutput = String.valueOf(loAveAge) + ":" + loAddress;
            context.write(key, new Text(loOutput));                     // uid <tab> age:address
        }
    }

    // STEP 3: Sort by average age
    private static class SortByAgeMapper
            extends Mapper<LongWritable, Text, TextPair, Text>
    {
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            String[] loLineOfData = value.toString().split("\\t");      // uid <tab> age:address
            String loCurrentUID = loLineOfData[0];
            String[] loAgeAndAddress = loLineOfData[1].split(":");
            String loAge = loAgeAndAddress[0];
            String loAddress = loAgeAndAddress[1];

            // same reducer and group, should define sort by loAge
            context.write(new TextPair("COMMON_KEY", loAge), new Text(loCurrentUID + ":" + loAddress));
                                                                        // KEY:age <tab> uid:address
        }
    }
    private static class SortByAgeReducer
            extends Reducer<TextPair, Text, Text, Text>
    {
        int obCount = 0;
        public void reduce(TextPair key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException
        {
            if (obCount == 20)
            {
                return;
            }

            String loAge = key.getSecond().toString();                  // different ages don't group to same iterator
            for (Text lpText : values)
            {
                String[] loUserAndAddress = lpText.toString().split(":");

                context.write(new Text(loUserAndAddress[0]), new Text(loUserAndAddress[1] + ", " + loAge));

                obCount++;
                if (obCount == 20)
                {
                    break;
                }
            }
        }
    }
    private static class SortByAgeDescending extends WritableComparator
    {
        protected SortByAgeDescending()
        {
            super(TextPair.class, true);
        }
        public int compare(WritableComparable w1, WritableComparable w2)
        {
            String loAge1 = ((TextPair) w1).getSecond().toString();
            String loAge2 = ((TextPair) w2).getSecond().toString();
            double cmp = Double.valueOf(loAge2) - Double.valueOf(loAge1);
            return cmp > 0 ? 1 : (cmp < 0 ? -1 : 0);
        }
    }

    // STEP 4: Join top 20 sorted result with address table, using in-memory join
    private static class JoinUserMapper
            extends Mapper<LongWritable, Text, TextPair, Text>
    {
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException
        {}
    }
    private static class JoinAddressMapper
            extends Mapper<LongWritable, Text, TextPair, Text>
    {
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException
        {}
    }
    private static class JoinUserAddressReducer
            extends Reducer<TextPair, Text, Text, Text>
    {
        public void reduce(TextPair key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException
        {}
    }


    // Driver program
    public static void main(String[] args) throws Exception
    {
        String[] otherArgs = getOtherArgs(args, 3, USAGE);

        // ==== STEP 1, using reduce-side join =====================
        Job loJobStepOne = setupMultiInputJob(
                JoinFriendMapper.class,
                JoinAgeMapper.class,
                JoinFriendAgeReducer.class,
                otherArgs[0],
                otherArgs[1],
                TEMP_FOLDER,
                TextPair.class,
                TextPair.class,
                "");
        loJobStepOne.setPartitionerClass(FirstPartitioner.class);
        loJobStepOne.setGroupingComparatorClass(GroupComparator.class);
        boolean loResult = loJobStepOne.waitForCompletion(true);

        // ==== STEP 2 =====================
        if (loResult)
        {
            Job loJobStepTwo = setupJob(
                    CalculateAveAgeMapper.class,
                    CalculateAveAgeReducer.class,
                    TEMP_FOLDER,
                    TEMP_FOLDER_ALTER,
                    "");
            loResult = loJobStepTwo.waitForCompletion(true);
        }
        else
        {
            System.exit(1);
        }

        // ==== STEP 3 =====================
        if (loResult)
        {
            Job loJobStepThree = setupJob(
                    SortByAgeMapper.class,
                    SortByAgeReducer.class,
                    TEMP_FOLDER_ALTER,
                    otherArgs[2],
                    "");
            loJobStepThree.setOutputKeyClass(TextPair.class);
            loJobStepThree.setPartitionerClass(FirstPartitioner.class);
            loJobStepThree.setSortComparatorClass(SortByAgeDescending.class);
            loResult = loJobStepThree.waitForCompletion(true);
        }
        else
        {
            System.exit(1);
        }

        /*
        // ==== STEP 4 =====================
        if (loResult)
        {
            Job loJobStep2 = setupMultiInputJob(
                    JoinFriendMapper.class,
                    JoinAgeMapper.class,
                    JoinFriendAgeReducer.class,
                    TEMP_FOLDER,
                    otherArgs[1],
                    TEMP_FOLDER_ALTER,
                    "");
            loResult = loJobStep2.waitForCompletion(true);
        }
        else
        {
            System.exit(1);
        }
*/
        System.exit(loResult ? 0 : 1);
    }
}
