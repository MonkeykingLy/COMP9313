
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;







public class TupleMR {
	
	public static class MMpairWritable implements WritableComparable<MMpairWritable>{
		private Text movie1;
		private Text movie2;
		
		public MMpairWritable() {
			this.movie1 = new Text("");
			this.movie2 = new Text("");
		}
		
		public MMpairWritable(Text movie1,Text movie2) {
			this.movie1=movie1;
			this.movie2=movie2;
		}

		public Text getMovie1() {
			return movie1;
		}

		public void setMovie1(Text movie1) {
			this.movie1 = movie1;
		}

		public Text getMovie2() {
			return movie2;
		}

		public void setMovie2(Text movie2) {
			this.movie2 = movie2;
		}
		
		

		@Override
		public int compareTo(MMpairWritable o) {
			// TODO Auto-generated method stub
			int result = movie1.toString().compareTo(o.getMovie1().toString());
			if(result!=0) {
				return result;
			}
			return movie2.toString().compareTo(o.getMovie2().toString());
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((movie1 == null) ? 0 : movie1.hashCode());
			result = prime * result + ((movie2 == null) ? 0 : movie2.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			MMpairWritable other = (MMpairWritable) obj;
			if (movie1 == null) {
				if (other.movie1 != null)
					return false;
			} else if (!movie1.equals(other.movie1))
				return false;
			if (movie2 == null) {
				if (other.movie2 != null)
					return false;
			} else if (!movie2.equals(other.movie2))
				return false;
			return true;
		}

		@Override
		public void readFields(DataInput data) throws IOException {
			// TODO Auto-generated method stub
			this.movie1.readFields(data);
			this.movie2.readFields(data);
		}

		@Override
		public void write(DataOutput data) throws IOException {
			// TODO Auto-generated method stub
			this.movie1.write(data);
			this.movie2.write(data);
		}

		@Override
		public String toString() {
			return "(" + this.movie1.toString() + "," + this.movie2.toString() + ")";
		}
		
	}
	
	public static class URRWritable implements Writable {
		private Text user;
		private Text rate1;
		private Text rate2;
		
		public URRWritable() {
			this.user=new Text("");
			this.rate1=new Text("");
			this .rate2 = new Text("");
		}
		
		public URRWritable(Text user,Text rate1,Text rate2) {
			this.user=user;
			this.rate1=rate1;
			this.rate2=rate2;
		}
		
		
		public Text getUser() {
			return user;
		}
		public void setUser(Text user) {
			this.user = user;
		}
		public Text getRate1() {
			return rate1;
		}
		public void setRate1(Text rate1) {
			this.rate1 = rate1;
		}
		public Text getRate2() {
			return rate2;
		}
		public void setRate2(Text rate2) {
			this.rate2 = rate2;
		}
		@Override
		public void readFields(DataInput data) throws IOException {
			// TODO Auto-generated method stub
			this.user.readFields(data);
			this.rate1.readFields(data);
			this.rate2.readFields(data);
			
		}
		@Override
		public void write(DataOutput data) throws IOException {
			// TODO Auto-generated method stub
			this.user.write(data);
			this.rate1.write(data);
			this.rate2.write(data);
			
		}
		@Override
		public String toString() {
			return "(" + this.user.toString() + "," + this.rate1.toString() + "," + this.rate2.toString() + ")";
		}
		
		
		

	}

	public static class MRpairWritable implements Writable {
		private Text movie;
		private Text rate;
		
		public MRpairWritable() {
			this.movie=new Text();
			this.rate = new Text();
		}
		
		public MRpairWritable(Text movie,Text rate) {
			this.movie = movie;
			this.rate = rate;
		}
		
		
		public Text getMovie() {
			return movie;
		}
		public void setMovie(Text movie) {
			this.movie = movie;
		}
		public Text getRate() {
			return rate;
		}
		public void setRate(Text rate) {
			this.rate = rate;
		}
		@Override
		public void readFields(DataInput data) throws IOException {
			// TODO Auto-generated method stub
			this.movie.readFields(data);
			this.rate.readFields(data);
			
		}
		@Override
		public void write(DataOutput data) throws IOException {
			// TODO Auto-generated method stub
			this.movie.write(data);
			this.rate.write(data);
			
		}
		@Override
		public String toString() {
			return this.movie.toString() + " " + this.rate.toString();
		}
		
		
		
	}
	
	public static class AListWritable extends ArrayWritable{
		public AListWritable() {
			super(Text.class);
			set(new Text[] {});
		}
		public AListWritable(Text[] aList) {
			this();
			set(aList);
		}

		public AListWritable(ArrayList<String> aList) {
			// TODO Auto-generated constructor stub
			this();
			int n = aList.size();
			Text[] textList = new Text[n];
			for(int i=0; i<n;i++) {
				textList[i]=new Text(aList.get(i));
			}
			set(textList);
		}
		
		public String toString() {
			Writable[] array = get();
			int n = array.length;
			
			StringBuilder aBuilder = new StringBuilder();
			for(int i=0;i<n;i++) {
				aBuilder.append(((Text)array[i]).toString());
			}
			aBuilder.insert(0, "[");
			aBuilder.append("]");
			String str = aBuilder.toString();
			return str;
		}
	}

	public static class MRarrayWritable extends ArrayWritable {
		public MRarrayWritable() {
			super(Text.class);
			set(new Text[] {});
		}
		public MRarrayWritable(Text[] aList) {
			this();
			set(aList);
		}

		public MRarrayWritable(ArrayList<String> aList) {
			// TODO Auto-generated constructor stub
			this();
			int n = aList.size();
			Text[] textList = new Text[n];
			for(int i=0; i<n;i++) {
				textList[i]=new Text(aList.get(i));
			}
			set(textList);
		}
		
		public String toString() {
			Writable[] array = get();
			int n = array.length;
			
			StringBuilder aBuilder = new StringBuilder();
			for(int i=0;i<n;i++) {
				aBuilder.append(((Text)array[i]).toString());
			}
			//aBuilder.insert(0, "[");
			//aBuilder.append("]");
			String str = aBuilder.toString();
			return str;
		}
	}
	
	public static class MyMapper extends Mapper<Object,Text,Text,MRpairWritable>{
		public static Text k = new Text();
		public static Text v = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, MRpairWritable>.Context context)
				throws IOException, InterruptedException {
					String line = value.toString();
					String[] info = line.split("::");
					String users = info[0];
					String movies = info[1];
					String marks = info[2];
					//String[] tokens = value.toString().split("::");
					//for(String token:tokens) {
					//	context.write(new Text(token), new IntWritable(1));
					//}
					k.set(users);
					//v.set(movies+" "+marks);
					MRpairWritable amr = new MRpairWritable(new Text(movies),new Text(marks));
					context.write(k, amr);
		}
	}
	
	public static class MyReducer extends Reducer<Text,MRpairWritable,Text,MRarrayWritable>{
		public static Text v = new Text();
		public static Text k = new Text();
		@Override
		protected void reduce(Text word, Iterable<MRpairWritable> values,
				Reducer<Text, MRpairWritable, Text, MRarrayWritable>.Context context) throws IOException, InterruptedException {
				ArrayList<String> aList = new ArrayList<String>();
			//StringBuilder vstr = new StringBuilder();
				for(MRpairWritable i:values) {
					aList.add(i.toString()+" ");
				}
				//v.set(vstr.toString());
				MRarrayWritable ama=new MRarrayWritable(aList);
				context.write(word, ama);
		}

		
	}
	
	public static class SecondMapper extends Mapper<Object,Text,MMpairWritable,URRWritable>{
		//public static Text k = new Text();
		//public static Text v = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, MMpairWritable, URRWritable>.Context context)
				throws IOException, InterruptedException {
					//HashMap<String,String> hashmap = new HashMap<String,String>();
					String line = value.toString();
					String[] info = line.split("	");
					String[] str = info[1].split(" ");
					for(int i=0;i<str.length-2;i+=2) {
						//System.out.println(info[i]);
						for(int j=i+2;j<str.length;j+=2) {
							int a = Integer.parseInt(str[i]);
							int b = Integer.parseInt(str[j]);
							if(a>b) {
								//k.set("("+str[i]+","+str[j]+")");
								MMpairWritable amm = new MMpairWritable(new Text(str[i]),new Text(str[j]));
								//v.set("("+info[0]+","+str[i+1]+","+str[j+1]+")");
								URRWritable aurr = new URRWritable(new Text(info[0]),new Text(str[i+1]),new Text(str[j+1]));
								context.write(amm, aurr);
							}
							else {
								//k.set("("+str[j]+","+str[i]+")");
								//v.set("("+info[0]+","+str[j+1]+","+str[i+1]+")");
								MMpairWritable bmm = new MMpairWritable(new Text(str[j]),new Text(str[i]));
								URRWritable burr = new URRWritable(new Text(info[0]),new Text(str[j+1]),new Text(str[i+1]));
								context.write(bmm, burr);
							}
							
						}
					}
			
		}
		
	}
	
	public static class SecondReducer extends Reducer<MMpairWritable,URRWritable,MMpairWritable,AListWritable>{
		public static Text v = new Text();
		@Override
		protected void reduce(MMpairWritable word, Iterable<URRWritable> values,
				Reducer<MMpairWritable, URRWritable, MMpairWritable, AListWritable>.Context context) throws IOException, InterruptedException {
				ArrayList<String> aList = new ArrayList<String>();
				for(URRWritable i:values) {
					if(aList.size()==0) {
						aList.add(i.toString());
					}
					else {
						aList.add(","+i.toString());
					}
				}
				AListWritable aText = new AListWritable(aList);
				context.write(word, aText);
		}

		
	}
		

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Path out =new Path(args[1]);
		
		
		Job jobone = Job.getInstance(conf, "TupleMR_First");
		
		jobone.setJarByClass(TupleMR.class);
		
		jobone.setMapperClass(MyMapper.class);
		jobone.setReducerClass(MyReducer.class);
		
		jobone.setMapOutputKeyClass(Text.class);
		jobone.setMapOutputValueClass(MRpairWritable.class);
		
		jobone.setOutputKeyClass(Text.class);
		jobone.setOutputValueClass(MRarrayWritable.class);
		
		jobone.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(jobone, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobone, new Path(out,"out1"));
		
		if(!jobone.waitForCompletion(true)) {
			System.exit(1);
		}
		
		Job jobtwo = Job.getInstance(conf, "TupleMR_Second");
		jobtwo.setJarByClass(TupleMR.class);
		
		jobtwo.setMapperClass(SecondMapper.class);
		jobtwo.setReducerClass(SecondReducer.class);
		
		jobtwo.setMapOutputKeyClass(MMpairWritable.class);
		jobtwo.setMapOutputValueClass(URRWritable.class);
		
		jobtwo.setOutputKeyClass(MMpairWritable.class);
		jobtwo.setOutputValueClass(AListWritable.class);
		
		jobtwo.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(jobtwo, new Path(out,"out1"));
		FileOutputFormat.setOutputPath(jobtwo, new Path(out,"out2"));
		
		if(!jobtwo.waitForCompletion(true)) {
			System.exit(1);
		}
		

	}

}
