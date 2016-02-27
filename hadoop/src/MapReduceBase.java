import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by xiezebin on 2/13/16.
 */
public class MapReduceBase
{
    protected final static String DRIVER_TO_MAPPER_KEY = "DRIVER_TO_MAPPER_KEY";

    // custom Pair, as composite key, used for reduce-side join
    protected static class TextPair implements WritableComparable<TextPair>
    {
        public Text obFirstText;
        public Text obSecondText;

        TextPair()
        {
            obFirstText = new Text();
            obSecondText = new Text();
        }
        TextPair(String arFriendID, String arMutualFriendID)
        {
            obFirstText = new Text(arFriendID);
            obSecondText = new Text(arMutualFriendID);
        }

        public Text getFirst() {
            return obFirstText;
        }
        public Text getSecond() {
            return obSecondText;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            obFirstText.write(dataOutput);
            obSecondText.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            obFirstText.readFields(dataInput);
            obSecondText.readFields(dataInput);
        }

        // compare by first value
        public int compareTo(TextPair arPair)
        {
            int cmp = obFirstText.compareTo(arPair.getFirst());
            if (cmp != 0)
            {
                return cmp;
            }
            else
            {
                return obSecondText.compareTo(arPair.getSecond());
            }
        }

        public String toString()
        {
            return obFirstText.toString() + ":" + obSecondText.toString();
        }
    }

    // partition and group by first value of TextPair
    public static class FirstPartitioner extends Partitioner<TextPair, Writable>
    {
        @Override
        public int getPartition(TextPair key, Writable value, int numPartitions)
        {
            return Math.abs(key.getFirst().hashCode() * 127) % numPartitions;         // mixing
        }
    }
    public static class GroupComparator extends WritableComparator
    {
        protected GroupComparator()
        {
            super(TextPair.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2)
        {
            TextPair p1 = (TextPair) w1;
            TextPair p2 = (TextPair) w2;
            return p1.getFirst().compareTo(p2.getFirst());
        }
    }


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
        /*
        String loTargetUIDs = otherArgs[numOfOtherArgs - 1];
        String[] loTargetUIDsp = loTargetUIDs.split(",");
        if (loTargetUIDsp.length != 2)
        {
            System.err.println(arUsage);
            System.exit(2);
        }
        */
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
        conf.set(DRIVER_TO_MAPPER_KEY, arDriverToMapperVal);

        // reuse output folder
        FileSystem loFS = FileSystem.get(new Configuration());
        loFS.delete(new Path(outputPath), true);


        // create a job with name CLASS_NAME
        Job job = new Job(conf, "JOB");
        job.setJarByClass(MapReduceBase.class);
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

        return job;
    }

    protected static Job setupMultiInputJob(
            Class<? extends Mapper> arMapClass1,
            Class<? extends Mapper> arMapClass2,
            Class<? extends Reducer> arReduceClass,
            String inputPath1,
            String inputPath2,
            String outputPath,
            Class<?> arOutputKeyClass,
            Class<?> arOutputValueClass,
            String arDriverToMapperVal)
            throws Exception
    {
        Configuration conf = new Configuration();
        // set argument
        conf.set(DRIVER_TO_MAPPER_KEY, arDriverToMapperVal);

        // reuse output folder
        FileSystem loFS = FileSystem.get(new Configuration());
        loFS.delete(new Path(outputPath), true);


        // create a job
        Job job = Job.getInstance(conf, "JOB");
        job.setJarByClass(MapReduceBase.class);
        job.setReducerClass(arReduceClass);

        // set output key type
        job.setOutputKeyClass(arOutputKeyClass);
        // set output value type
        job.setOutputValueClass(arOutputValueClass);
        //set the HDFS path of the input data
        MultipleInputs.addInputPath(job, new Path(inputPath1), TextInputFormat.class, arMapClass1);
        MultipleInputs.addInputPath(job, new Path(inputPath2), TextInputFormat.class, arMapClass2);
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }
}
