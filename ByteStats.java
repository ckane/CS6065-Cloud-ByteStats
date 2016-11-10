import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;

public class ByteStats {
  public static class ByteStatsMapper extends Mapper<Object,BytesWritable,ByteWritable,LongWritable> {
    public void map(Object key, BytesWritable value, Context context)
        throws IOException, InterruptedException {
      for(byte v : value.getBytes()) {
        context.write(new ByteWritable(v), new LongWritable(1L));
      };
    };
  };

  public static class ByteStatsReducer extends Reducer<ByteWritable,LongWritable,ByteWritable,LongWritable> {
    public void reduce(ByteWritable key, Iterable<LongWritable> values, Context context) 
        throws IOException, InterruptedException {
      long sum = 0;
      for(LongWritable i : values) {
        sum += i.get();
      };
      context.write(key, new LongWritable(sum));
    };
  };

  public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Byte Analysis");
    job.setJar("randdata.jar");
    job.setJarByClass(ByteStats.class);
    job.setMapperClass(ByteStatsMapper.class);
    job.setCombinerClass(ByteStatsReducer.class);
    job.setReducerClass(ByteStatsReducer.class);
    job.setOutputKeyClass(ByteWritable.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    job.setInputFormatClass(BlockRecordInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  };
};
