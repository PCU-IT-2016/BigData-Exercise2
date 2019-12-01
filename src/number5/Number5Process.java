package number5;

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
import java.util.HashMap;

public class Number5Process {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private IntWritable one = new IntWritable(1);
        private Text key = new Text();
        private Text value = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String regex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
            String[] lineValues = value.toString().split(regex);

            if (lineValues.length == 16) {
                String neighbourhood = lineValues[5];
                this.key.set(neighbourhood);

                // value = ",HostId:1"
                this.value.set(lineValues[2] + ":1");
                context.write(this.key, this.value);
            }
        }
    }

    public static class RichestInNeighbourhoodReducer extends Reducer<Text,Text,Text,NullWritable> {
        private Text result = new Text();
        private NullWritable out = NullWritable.get();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            HashMap <String, Integer> map = new HashMap<>();

            // value = "HostId:1"
            for (Text value : values) {
                String[] lineValues = value.toString().split(":");
                if (map.containsKey(lineValues[0])) {
                    map.put(lineValues[0], map.get(lineValues[0]) + Integer.parseInt(lineValues[1]));
                } else {
                    map.put(lineValues[0], Integer.parseInt(lineValues[1]));
                }
            }

            // get max value in map
            int maxValue = -1;
            String maxHostIds = "";
            for (Object k : map.keySet().toArray()) {

                String tempKey = (String) k;
                int currentKeyVal = map.get(k);

                // jumlah value sama.
                if (currentKeyVal == maxValue) {
                    maxHostIds += tempKey + ":";
                    continue;
                }

                // ada yang lebih tinggi
                if (currentKeyVal > maxValue) {
                    maxHostIds = tempKey + ":";
                    maxValue = currentKeyVal;
                }
            }

            maxHostIds = maxHostIds.substring(0, maxHostIds.length() - 1);

            result.set(key.toString() + "," + maxHostIds);
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

        Job job = Job.getInstance(conf, "The richest in Neighborhood");

        job.setJarByClass(Number5Process.class);

        job.setMapperClass(Number5Process.TokenizerMapper.class);
        job.setReducerClass(Number5Process.RichestInNeighbourhoodReducer.class);

        // MAPPER KEY & VALUE CLASS
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);

    }
}
