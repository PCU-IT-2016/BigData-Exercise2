package Processes;

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

public class Number2Process {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private IntWritable one = new IntWritable(1);
        private Text neighbourhoodText = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String regex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
            String[] lineValues = value.toString().split(regex);

                String neighbourhood = lineValues[5];
                this.neighbourhoodText.set(neighbourhood);

                context.write(this.neighbourhoodText, this.one);
        }
    }

    public static class SumMeanReducer extends Reducer<Text,IntWritable,Text,NullWritable> {
        private Text result = new Text();
        private NullWritable out = NullWritable.get();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(key + "," + String.valueOf(sum));
            context.write(result, out);
        }
    }

    public void run(String inputPath, String outputPath) throws Exception {
        // handles "path already exist" exception.
        try {
            FileUtils.cleanDirectory(new File(outputPath));
            FileUtils.deleteDirectory(new File(outputPath));
        } catch (Exception e) {
            e.printStackTrace();
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Neighbourhood Count");

        job.setJarByClass(Number2Process.class);

        job.setMapperClass(Number2Process.TokenizerMapper.class);
        job.setReducerClass(Number2Process.SumMeanReducer.class);

        // MAPPER KEY & VALUE CLASS
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);

    }
}
