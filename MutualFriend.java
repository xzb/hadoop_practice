import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;

/**
 * Created by xiezebin on 2/5/16.
 */
public class MutualFriend
{
    private final static String sTargetUIDKey = "PARAMETER";
    private final static String sIsFriend = "-1";

    // custom Pair, not comparable
    private static class CandidateFriend implements Writable
    {
        public String obFriend;
        public String obMutualFriend;
        public Text obFriendText;
        public Text obMutualFriendText;

        CandidateFriend()
        {
            obFriendText = new Text();
            obMutualFriendText = new Text();
        }
        CandidateFriend(String arFriendID, String arMutualFriendID)
        {
            obFriend = arFriendID;
            obMutualFriend = arMutualFriendID;
            obFriendText = new Text(arFriendID);
            obMutualFriendText = new Text(arMutualFriendID);
        }

        public String getFriend() {
            return obFriend;
        }

        public String getMutualFriend() {
            return obMutualFriend;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            obFriendText.write(dataOutput);
            obMutualFriendText.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            obFriendText.readFields(dataInput);
            obMutualFriendText.readFields(dataInput);
            obFriend = obFriendText.toString();
            obMutualFriend = obMutualFriendText.toString();
        }
    }


    public static class Map
            extends Mapper<LongWritable, Text, Text, CandidateFriend>
    {
        private Text obKeyOut = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            // the target user to whom we recommend friends
            String loTargetUID = context.getConfiguration().get(sTargetUIDKey);
            obKeyOut.set(loTargetUID);

            String[] loLineOfData = value.toString().split("\\t");
            if (loLineOfData.length < 2)
            {
                return;
            }

            String loCurrentUID = loLineOfData[0];
            String loFriends = loLineOfData[1];
            String[] loFriendID = loFriends.split(",");
            int len = loFriendID.length;

            // case 1: target user direct friend
            if (loCurrentUID.equals(loTargetUID))
            {
                for (int i = 0; i < len; i++)
                {
                    CandidateFriend loCandidate = new CandidateFriend(loFriendID[i], sIsFriend);
                    context.write(obKeyOut, loCandidate);
                }
                return;
            }

            // case 2: current user and the friend list don't have target user, just discard
            boolean hasTargetUID = false;
            for (int i = 0; i < len; i++)
            {
                if (loFriendID[i].equals(loTargetUID))
                {
                    hasTargetUID = true;
                    break;
                }
            }
            if (!hasTargetUID)
            {
                return;
            }

            // case 3: emit target user and his candidate friend, mutual friend is current user
            for (int i = 0; i < len; i++)
            {
                if (!loFriendID[i].equals(loTargetUID))
                {
                    CandidateFriend loCandidate = new CandidateFriend(loFriendID[i], loCurrentUID);
                    context.write(obKeyOut, loCandidate);
                }
            }
        }
    }

//    public static class MyComparator implements RawComparator<CandidateFriend>
//    {
//        public int compare(CandidateFriend cand1, CandidateFriend cand2)
//        {
//            return 1;
//        }
//        public int compare(byte[] var1, int var2, int var3, byte[] var4, int var5, int var6)
//        {
//            return 1;
//        }
//    }

    public static class Reduce
            extends Reducer<Text, CandidateFriend, Text, Text> {

        private Text obValueOut = new Text();

        public void reduce(Text key, Iterable<CandidateFriend> values, Context context
        ) throws IOException, InterruptedException {

            java.util.Map<String, Integer> loUnsortedCandidates = new HashMap<>();

            for (CandidateFriend lpCandidateFriend : values)
            {
                String loCandidate = lpCandidateFriend.getFriend();
                String loMutualFriend = lpCandidateFriend.getMutualFriend();

                if (loUnsortedCandidates.containsKey(loCandidate))
                {
                    int loCount = loUnsortedCandidates.get(loCandidate);
                    if (loCount == -1)
                    {
                        // ignore direct friend
                        continue;
                    }
                    else
                    {
                        loUnsortedCandidates.put(loCandidate, loCount + 1);
                    }
                }
                else
                {
                    if (loMutualFriend.equals(sIsFriend))
                    {
                        loUnsortedCandidates.put(loCandidate, -1);
                    }
                    else
                    {
                        loUnsortedCandidates.put(loCandidate, 1);
                    }
                }
            }


            // sort the candidates by the count of mutual friends with target user
            java.util.Map<Integer, String> loSortedCandidates = new TreeMap<>();
            for (java.util.Map.Entry<String, Integer> entry : loUnsortedCandidates.entrySet())
            {
                loSortedCandidates.put(entry.getValue(), entry.getKey());
            }

            int loTillTen = 0;
            StringBuilder loBuilder = new StringBuilder();
            for (java.util.Map.Entry<Integer, String> entry : loSortedCandidates.entrySet())
            {
                loBuilder.append(",");
                loBuilder.append(entry.getValue());
                loTillTen++;
                if (loTillTen == 10)
                {
                    break;
                }
            }
            loBuilder.deleteCharAt(0);
            obValueOut.set(loBuilder.toString());

            context.write(key, obValueOut);
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 3) {
            System.err.println("Usage: WordCount <in> <out> <user id>");
            System.exit(2);
        }

        // save target user id
        String targetUID = otherArgs[2];
        conf.set(sTargetUIDKey, targetUID);

        // create a job with name "wordcount"
        Job job = new Job(conf, "MutualFriend");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        //job.setSortComparatorClass(MyComparator.class);

        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);


        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(CandidateFriend.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}


