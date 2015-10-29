package org.apache.hadoop.hadoop_core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class cooccur {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
		  System.err.println("Usage: wordcount <in> <out> <ngram>");
		  System.exit(2);
		    }
		Job job = new Job(conf, "word count");
		job.setJarByClass(cooccur.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumcom.class);
		job.setReducerClass(IntSumcom.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(2); // 2 reducers
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

public static class TokenizerMapper
   extends Mapper<Object, Text, Text, IntWritable>{

private final static IntWritable one = new IntWritable(1);
private Text word = new Text();
String art_no_t ="";

String year_t="";
String b="";
////////////////////////////////select ngram
String before[] = {"","",""};

public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
{

	int W_S = 10 ; //윈도우 사이즈
	List<Object> words= new ArrayList<Object>();
  StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase(),"\n\t,.()'\"-]*;ㅁ ");
  while(itr.hasMoreTokens()){
      String match = "[^\uAC00-\uD7A3xfe0-9a-zA-Z\\s]"; // 유효성검사
      String a = itr.nextToken().replaceAll(match, "");
	  if(a.contains("yearck") == false && a.contains("artnck") ==false)
      words.add(a); //워드리스트에 단어별로 싹다넣는다.
  }
  
  for(int i = 0 ; i < words.size() ; i++)
  {
	  int sum = 0;
	  for(int j = 0 ; j < W_S ; j++)
	  {
		  if (i+j >= words.size())
		  {
			  break;
		  }
		  if(words.get(i).toString().equals(words.get(i+j).toString()) == false )
		  {
			  word.set(words.get(i)+"\t"+words.get(i+j));
			  context.write(word, one);//////////////////
			  sum++;
		  }
	  }
	  if (sum>0)
	  {
		  //word.set(words.get(i)+"\t*\t"+sum);
		  //context.write(word, one);////////////////////
	  }
  }
  }
}
public static class IntSumcom
extends Reducer<Text,IntWritable,Text,IntWritable> {
private IntWritable result = new IntWritable();

public void reduce(Text key, Iterable<IntWritable> values,
                Context context
                ) throws IOException, InterruptedException {
int sum = 0;
for (IntWritable val : values) {
	sum+=val.get();
	}
	result.set(sum);
	context.write(key, result);
	}
}
//public static class IntSumReducer
//   extends Reducer<Text,IntWritable,Text,IntWritable> {
//private IntWritable result = new IntWritable();
//private int word1_sum=0;
//private int key_count = 0;
//private String prev_key = "null";
////public void reduce(Text keyin, Iterable<IntWritable> values,
////                   Context context
////                   ) throws IOException, InterruptedException, ArrayIndexOutOfBoundsException {
////
////	String []temp = (keyin.toString()).split("\t");
////	String word1 = temp[0];
////	String word2 = temp[1];
////	int val = Integer.parseInt(temp[2]);
////
////	String key = word1 +"\t"+ word2;
////	double hybrid_f = 0.0;
////	
////
////	
////	if(prev_key.equals(key) == false) // 현재키가 이전키와 같이 아나면
////	{
////		if(prev_key.equals("null")==false) // 이전키가 존재한다면 (처음빼고는 다존재함)
////		{
////			String temp1[] = prev_key.split("\t");                
////			word1 = temp1[0]; //이전키를 삽입
////			word2 = temp1[1];
////			if(word2.equals("*")) // 이전키가 총합 키라면 
////			{
////				word1_sum = key_count;  //이전키의 키카운터를 word1의 총합으로 생각
////				result.set(word1_sum);
////				keyin = new Text(word1+"\t"+word2);
////				//context.write(keyin, result); // 총합키 출력( 가을하늘	*	9) 이런것만 출력되는부분
////			}
////			else if(key_count >= 1) // 이전키의 키카운터가 존재한다면 (keycount는 이전 값을 담고있다.) 
////			{
////				hybrid_f = (key_count) * (100 * key_count/word1_sum);
////				keyin = new Text(word1+"\t"+word2);
////				result.set((int)hybrid_f);
////				context.write(keyin, result);
////			}
////		}
////		key_count = val; // 현재 val값을 넣는다 다음 포문에서 이전값으로 참조하기위해서
////		prev_key = key; //현재 키값이 이전키값으로 간다.
////	}
////	else // 이전키와 현재 키가 같다면
////	{
////		key_count = key_count+val;
////	}
//////	String temp2[] = prev_key.split("\t"); 
//////	if(temp2.length == 2){word1 = temp2[0];word2 = temp2[1];}
//////	if (key_count>=0)
//////	{
//////		keyin = new Text(word1 + "\t"+word2 +"\t"+key_count+"\t"+(float)(key_count)/word1_sum);
//////		result.set(word1_sum);
//////		context.write(keyin, result);
//////	}
////  }
//}
}