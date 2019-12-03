package Processes;

import Processes.Entity.AirbnbRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class Number4Process {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private Text key = new Text();
        private IntWritable value = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            AirbnbRecord record = new AirbnbRecord();
            record.decode(value.toString());

            this.key.set(record.getRoomType() + ":" + record.getHostId());
            this.value.set(record.getMinimumNights());

            context.write(this.key, this.value);
        }
    }

    public static class HostIdMinimumNightReducer extends Reducer<Text, IntWritable, Text, NullWritable>
    {
        private Text result = new Text();
        private NullWritable out = NullWritable.get();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            String room_type = key.toString();

            int minimum_night = 0;
            int count = 0;

            for (IntWritable value : values)
            {
                minimum_night += value.get();
                count += 1;
            }

            float mean = minimum_night / count;
            this.result.set(room_type + "," + "Minimun Night: " + String.valueOf(mean));

            context.write(this.result, this.out);
        }
    }

    public void run(String inputPath, String outputPath) throws Exception
    {
        // handles "path already exist" exception.
        try
        {
            FileUtils.cleanDirectory(new File(outputPath));
            FileUtils.deleteDirectory(new File(outputPath));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Minimum Night");

        job.setJarByClass(Number4Process.class);

        job.setMapperClass(Number4Process.TokenizerMapper.class);
        job.setReducerClass(Number4Process.HostIdMinimumNightReducer.class);

        // MAPPER KEY & VALUE CLASS
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);

    }
}
