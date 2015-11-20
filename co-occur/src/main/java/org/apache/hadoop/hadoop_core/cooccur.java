package org.apache.hadoop.hadoop_core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.math.BigInteger;
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
//		job.setReducerClass(IntSumcom.class);
		job.setCombinerClass(IntSumcom.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
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
	StringTokenizer itr1 = new StringTokenizer(value.toString().toLowerCase(),"1234567890\n\t,.()'\"-]*;ㅁ ");

  while(itr1.hasMoreTokens()){
      String match = "[^\uAC00-\uD7A3xfe0-9a-zA-Z\\s]"; // 유효성검사
      String a = itr1.nextToken().replaceAll(match, "");
	  if(a.contains("yearck") == false && a.contains("artnck") ==false)
      words.add(a); //워드리스트에 단어별로 싹다넣는다.
	  word.set("*\t"+a);
	  context.write(word, one);
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
//		  word.set(words.get(i)+"\t*\t"+sum);
//		  word.set("*\t"+words.get(i)+"\t"+sum);
//		  context.write(word, one);////////////////////
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
public static double logB(double x, double base) {
    return Math.log(x) / Math.log(base);
  }

public static class IntSumReducer
extends Reducer<Text,IntWritable,Text,IntWritable> {
private IntWritable result = new IntWritable();
String wordsum[]= new String[50000000];
int count = 0;
public void reduce(Text keyin, Iterable<IntWritable> values,
              Context context
              ) throws IOException, InterruptedException, ArrayIndexOutOfBoundsException {
int sum=0;
	for (IntWritable val : values) {
		sum+=val.get();
		}
   String []temp = (keyin.toString()).split("\t");// 키인을 탬프에 넣어서 word1 word2 value로 나눔
if(temp.length == 2){
	
String word1 = temp[0]; //* // 가을하늘
String word2 = temp[1]; //충성을 // 공활한데
int i1 = 0;
int i2 = 0;
int a1=0;
int a2=0;
int value = sum; //1 //3 이런식으로들어감

//////////////////////////////////해쉬함수/////////////////////////
int adress = 0;
for(int i = 0 ; i < word1.length() ; i++) // 단어의 개수만큼 돈다
{
		int c = word1.charAt(i);
		c = c - 85;
			if(i %2 == 0)
			{
				adress= adress + c*100;
			}
			else adress= adress + c;
		
}
a1 = adress % 50000000;
adress=0;
for(int i = 0 ; i < word2.length() ; i++) // 단어의 개수만큼 돈다
{
		int c = word2.charAt(i);
		c = c - 85;
			if(i %2 == 0)
			{
				adress= adress + c*100;
			}
			else adress = adress + c;
		
}
//////////////////////////////////해쉬함수/////////////////////////
a2 = adress%50000000;


if(word1.equals("*") ) // 각토큰의 빈도수를 wordsum에 저장 wordval에 빈도수저장.
{
	
	if(wordsum[a2] == null)
		wordsum[a2] = word2+"\t"+value+"\t";
	else
		wordsum[a2] = wordsum[a2] + word2+"\t"+value+"\t";// 충성을 
	
	count = count + value;
}
else if( wordsum[a2] != null &&  wordsum[a1] != null )
{
	/////////////해쉬주소로 배열에서 원하는값 찾는 부분////////////
	String []temp1 = wordsum[a1].split("\t");
	for(int i = 0 ; i < temp1.length ; i = i+2)
	{
		if(temp1[i].equals(word1))
		{i1 = i;
			break;
		}
	}
	String []temp2 = wordsum[a2].split("\t");
	for(int i = 0 ; i < temp2.length ; i = i+2)
	{
		if(temp2[i].equals(word2))
		{
			i2 = i;
			break;
		}
	}
	
	int word1_val = Integer.parseInt(temp1[i1+1]); // P(word1) // 워드1의 확률
	int word2_val = Integer.parseInt(temp2[i2+1]);// P(word2) // 워드2의 확률
	
	/////////////해쉬주소로 배열에서 원하는값 찾는 부분////////////
   BigInteger big = new BigInteger("1"); // 숫자가 너무 크므로 빅인테져에 답는다
   
   ///////////////////////////pmi 구하는 공식////////////////////
   big = big.multiply(BigInteger.valueOf(value));
   big = big.multiply(BigInteger.valueOf(count));
   big = big.multiply(BigInteger.valueOf(100));
   big = big.divide(BigInteger.valueOf(word1_val));
   big = big.divide(BigInteger.valueOf(word2_val));
   ///////////////////////////////////////////////////////////
   String big_to_string = ""+big; // 빅을 더블로 바꿔준다 빅인테져는 바로 더블에 못담으므로 스트링을 한번 거친다
   
   if (big_to_string.length() > 4){
	   big_to_string = big_to_string.substring(0, 4);
   }
   double pmi = Double.parseDouble(big_to_string); // 더블에 담는다
   pmi = pmi / 100;
   pmi = Math.log(pmi)/Math.log(2.0); //밑이2인 로그를만들기위해
   //keyin = new Text("count : " +count+"\tvalue:"+value+"\tpmi:"+pmi +"\t" + word1_val +"\tword2 : " + word2 +"\t"+ word2_val+"\t"+wordsum[a2]);
   keyin = new Text( word1 +"\t"+ word2+"\t"+pmi);

   context.write(keyin, result);
}
}
}
}

}

