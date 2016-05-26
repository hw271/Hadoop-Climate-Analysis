import java.io.IOException;
import java.util.StringTokenizer;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper
        extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().toLowerCase().replaceAll("[^a-zA-Z0-9]",""));
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
        extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
                           ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class Top10Mapper
        extends Mapper<Object, Text, IntWritable, Object>{
        
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            PriorityQueue<WordCountPair> queue = new PriorityQueue<WordCountPair>(11);
            while (itr.hasMoreTokens()) {
                String tokens = itr.nextToken();
                if(!tokens.contains("\t")) continue;
                String[] wc = tokens.split("\t");
                WordCountPair pair = new WordCountPair(wc[0], Integer.valueOf(wc[1]));
                queue.add(pair);
                if(queue.size()>10){
                    queue.poll();
                }
            }
            context.write(one, queue);
        }
    }

    public static class Top10Reducer
        extends Reducer<IntWritable,Object,Text,IntWritable> {

        private Text word = new Text();
        private IntWritable count = new IntWritable();
        
        public void reduce(IntWritable key, Iterable<Object> values,
                           Context context
                           ) throws IOException, InterruptedException {

            PriorityQueue<WordCountPair> result = new PriorityQueue<WordCountPair>(11);
            for(Object value: values){
                PriorityQueue<WordCountPair> queue = (PriorityQueue<WordCountPair>) value;    
                for(WordCountPair pair:queue){
                    result.add(pair);
                    if(result.size()>10){
                        result.poll();
                    }
                }
            }
            
            while(result.size()>0){
                WordCountPair pair = result.poll();
                word.set(pair.word);
                count.set(pair.count);
                context.write(word, count);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);   
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("./temp"));
        job.waitForCompletion(true);


        Job job1 = Job.getInstance(conf, "top10");
        job1.setJarByClass(WordCount.class);
        job1.setMapperClass(Top10Mapper.class);
        job1.setCombinerClass(Top10Reducer.class);
        job1.setReducerClass(Top10Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path("./temp"));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}

class WordCountPair implements Comparable<WordCountPair>{
    String word;
    int count;
    public WordCountPair(String word, int count){
        this.word = word;
        this.count = count;
    }

    public int compareTo(WordCountPair o1){
        if(this.count<o1.count) return -1;
        else if(this.count==o1.count){
            return this.word.compareTo(o1.word);
        }else return 1;
    }
}
