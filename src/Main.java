import java.io.IOException;
import java.util.StringTokenizer;

import hostmean.HostMeanProcess;
import neighbourhoodcount.NCProcess;
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

public class Main {

    public static void main(String[] args) throws Exception {
//        HostMeanProcess hostMeanProcess = new HostMeanProcess();
//        hostMeanProcess.run("input/airbnb", "output/hostmean");

        NCProcess ncProcess = new NCProcess();
        ncProcess.run("input/airbnb", "output/neighbourhoodcount");
    }

}