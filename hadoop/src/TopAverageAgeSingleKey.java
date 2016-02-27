import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by xiezebin on 2/19/16.
 */
public class TopAverageAgeSingleKey extends MapReduceBase
{
    private final static String USAGE = "Usage: TopAverageAge <in_adj> <in_data> <out>";
    private final static String TEMP_FOLDER = "/zxx140430/output/temp/1";
    private final static String TEMP_FOLDER_ALTER = "/zxx140430/output/temp/2";

    private final static String CURRENT_YEAR = "2016";
    private final static String CURRENT_MONTH = "2";

    // STEP 1: Join two tables by key of each friend in list
    private static class JoinFriendMapper
            extends Mapper<LongWritable, Text, Text, Text>
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
                context.write(new Text(loFriend[i]), new Text("A," + loCurrentUID));
            }
        }
    }
    private static class JoinAgeMapper
            extends Mapper<LongWritable, Text, Text, Text>
    {
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            String[] loLineOfData = value.toString().split(",");

            String loCurrentUID = loLineOfData[0];
            String loDateOfBirth = loLineOfData[9];
            double loAge = Integer.valueOf(CURRENT_YEAR) - Integer.valueOf(loDateOfBirth.split("/")[2]);
//                    + (Integer.valueOf(CURRENT_MONTH) - Integer.valueOf(loDateOfBirth.split("/")[0])) * 1.0 / 12;

            context.write(new Text(loCurrentUID), new Text("B," + String.valueOf(loAge)));
        }
    }
    private static class JoinFriendAgeReducer
            extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException
        {
            String loAge = "0";

            // reduce-side 1-many join, first find age
            for (Text value: values)
            {
                String[] loTagAndContent = value.toString().split(",");
                if ("B".equals(loTagAndContent[0]))
                {
                    loAge = loTagAndContent[1];
                    break;
                }
            }
            for (Text value: values)
            {
                String[] loTagAndContent = value.toString().split(",");
                if ("A".equals(loTagAndContent[0]))
                {
                    context.write(new Text(loTagAndContent[1]), new Text(loAge));
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
            String loFriendAge = loLineOfData[1];

            context.write(new Text(loCurrentUID), new Text(loFriendAge));
        }
    }
    private static class JoinAddressMapper
            extends Mapper<LongWritable, Text, Text, Text>
    {
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            String[] loLineOfData = value.toString().split(",");

            String loCurrentUID = loLineOfData[0];
            String loAddress = loLineOfData[3] + ", " + loLineOfData[4] + ", " + loLineOfData[5];

            context.write(new Text(loCurrentUID), new Text(String.valueOf(loAddress)));
        }
    }
    private static class CalculateAveAgeReducer
            extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException
        {
            String loAddress = "";
            double loAgeSum = 0;
            int loCount = 0;

            for (Text loText : values)
            {
                String loContent = loText.toString();
                if (loContent.indexOf(",") > 0)
                {
                    // this is an address
                    loAddress = loContent;
                }
                else
                {
                    loAgeSum += Double.valueOf(loContent);
                    loCount++;
                }
            }
            double loAveAge = loAgeSum * 1.0 / loCount;
            String loOutput = String.format("%.2f", loAveAge) + ":" + loAddress;
            context.write(key, new Text(loOutput));                     // uid <tab> age:address
        }
    }

    // STEP 3: Sort by average age
    private static class SortByAgeMapper
            extends Mapper<LongWritable, Text, DoubleWritable, Text>
    {
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            String[] loLineOfData = value.toString().split("\\t");      // uid <tab> age:address
            String loCurrentUID = loLineOfData[0];
            String[] loAgeAndAddress = loLineOfData[1].split(":");
            double loAge = Double.valueOf(loAgeAndAddress[0]);
            String loAddress = loAgeAndAddress[1];

            context.write(new DoubleWritable(loAge), new Text(loCurrentUID + ":" + loAddress));     //age <tab> uid:address
        }
    }
    private static class SortByAgeReducer
            extends Reducer<DoubleWritable, Text, Text, Text>
    {
        int obCount = 0;

        public void reduce(DoubleWritable key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException
        {
            if (obCount == 20)
            {
                return;
            }

            for (Text value : values)
            {
                String[] loLineOfData = value.toString().split("\\t");
                String loAveAge = loLineOfData[0];
                String[] loUidAndAddress = loLineOfData[1].split(",");

                context.write(new Text(loUidAndAddress[0]), new Text(loUidAndAddress[1] + ", " + loAveAge));
                obCount++;
                if (obCount == 20)
                {
                    break;
                }
            }
        }
    }
    private static class SinglePartitioner extends Partitioner<Writable, Writable>
    {
        @Override
        public int getPartition(Writable key, Writable value, int numPartitions)
        {
            return 0;
        }
    }
    private static class SortByAgeDescending extends WritableComparator
    {
//        protected SortByAgeDescending()
//        {
//            super();
//        }
        public int compare(WritableComparable w1, WritableComparable w2)
        {
            return - w1.compareTo(w2);
        }
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
                Text.class,
                Text.class,
                "");
        boolean loResult = loJobStepOne.waitForCompletion(true);

        // ==== STEP 2 =====================
        if (loResult)
        {
            Job loJobStepTwo = setupMultiInputJob(
                    CalculateAveAgeMapper.class,
                    JoinAddressMapper.class,
                    CalculateAveAgeReducer.class,
                    TEMP_FOLDER,
                    otherArgs[1],
                    TEMP_FOLDER_ALTER,
                    Text.class,
                    Text.class,
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
            loJobStepThree.setOutputKeyClass(Text.class);
            loJobStepThree.setPartitionerClass(SinglePartitioner.class);
            loJobStepThree.setSortComparatorClass(SortByAgeDescending.class);
            loResult = loJobStepThree.waitForCompletion(true);
        }
        else
        {
            System.exit(1);
        }


        System.exit(loResult ? 0 : 1);
    }
}
