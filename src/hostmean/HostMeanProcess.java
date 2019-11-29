package hostmean;

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
import java.util.StringTokenizer;

public class HostMeanProcess {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private IntWritable priceIntWritable;
        private Text hostId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String regex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
            String[] lineValues = value.toString().split(regex);

            if (lineValues.length == 16) {
                String hostId = lineValues[2];
                this.hostId.set(hostId);

                String priceInString = lineValues[9];
                this.priceIntWritable = new IntWritable(Integer.valueOf(priceInString));

                context.write(this.hostId, this.priceIntWritable);
            }
        }
    }

    public static class SumMeanReducer extends Reducer<Text,IntWritable,Text,NullWritable> {
        private Text result = new Text();
        private NullWritable out = NullWritable.get();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int len = 0;
            for (IntWritable val : values) {
                sum += val.get();
                len += 1;
            }
            float mean = sum / len;
            result.set(key + "," + String.valueOf(mean));
            context.write(result, out);
        }
    }

    public void run(String inputPath, String outputPath) throws Exception {
        // handles "path already exist" exception.
        FileUtils.cleanDirectory(new File(outputPath));
        FileUtils.deleteDirectory(new File(outputPath));

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Host Mean");

        job.setJarByClass(HostMeanProcess.class);

        job.setMapperClass(HostMeanProcess.TokenizerMapper.class);
        job.setReducerClass(SumMeanReducer.class);

        // MAPPER KEY & VALUE CLASS
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
