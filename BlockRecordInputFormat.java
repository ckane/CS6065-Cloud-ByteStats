import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;

public class BlockRecordInputFormat extends FileInputFormat<Object,BytesWritable> {
  public RecordReader<Object,BytesWritable> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new BlockRecordReader();
  };

  public class BlockRecordReader extends RecordReader<Object,BytesWritable> {
    private Object key = null;
    private BytesWritable val = null;
    FSDataInputStream fsd = null;
    boolean split_done = false;
    byte input[] = null;
    long start = 0;
    long pos = 0;
    long end = 0;
    int chunk_size = 0;
    int block_max = 2048;
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws
        IOException, InterruptedException {
      FileSplit fsplit = (FileSplit)split;

      final Path filepath = fsplit.getPath();
      FileSystem fs = filepath.getFileSystem(context.getConfiguration());
      fsd = fs.open(filepath);
      if(fsplit.getLength() > block_max) {
        chunk_size = block_max;
      } else {
        chunk_size = (int)fsplit.getLength();
      }
      input = new byte[chunk_size];
      pos = start = fsplit.getStart();
      end = pos + fsplit.getLength();
    };

    @Override
    public Object getCurrentKey() {
      return key;
    };

    @Override
    public BytesWritable getCurrentValue() {
      return val;
    };

    @Override
    public void close() throws IOException {
      fsd.close();
    };

    @Override
    public boolean nextKeyValue() throws IOException {
      if(pos >= end) {
        return false;
      }

      fsd.readFully(pos, input);
      val = new BytesWritable(input);
      pos += input.length;

      return true;
    };

    @Override
    public float getProgress() throws IOException {
      return ((float)pos - (float)start)/((float)end - (float)start);
    };
  };
}
