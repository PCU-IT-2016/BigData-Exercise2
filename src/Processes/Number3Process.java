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

public class Number3Process {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text key = new Text();
        private IntWritable value = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            AirbnbRecord record = new AirbnbRecord();
            record.decode(value.toString());

            this.key.set(record.getRoomType());
            this.value.set(record.getPrice());

            context.write(this.key, this.value);
        }
    }

    public static class RoomTypePriceReducer extends Reducer<Text,IntWritable,Text,NullWritable> {
        private Text result = new Text();
        private NullWritable out = NullWritable.get();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String roomType = key.toString();

            float maxPrice = Float.NEGATIVE_INFINITY;
            float minPrice = Float.POSITIVE_INFINITY;
            int totalPrice = 0;
            int count = 0;

            for (IntWritable value : values) {
                int price = value.get();
                totalPrice += price;
                if (price > maxPrice) {
                    maxPrice = price;
                }
                if (price < minPrice) {
                    minPrice = price;
                }
                count += 1;
            }
            float mean = totalPrice / count;
            this.result.set(roomType + "," + String.valueOf(mean) + ","
                    + String.valueOf(maxPrice) + "," + String.valueOf(minPrice));

            context.write(this.result, this.out);
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

        job.setJarByClass(Number3Process.class);

        job.setMapperClass(Number3Process.TokenizerMapper.class);
        job.setReducerClass(Number3Process.RoomTypePriceReducer.class);

        // MAPPER KEY & VALUE CLASS
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);

    }
}
