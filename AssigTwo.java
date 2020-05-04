
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import javax.sound.sampled.Line;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;


import scala.Tuple2;
import scala.reflect.internal.Trees.New;
import scala.sys.process.ProcessBuilderImpl.AndBuilder;



public class AssigTwoz5133109 {
	public static class Terminal implements Serializable{
		private String destination;
		private Integer mark;
		private String road;
		
		public Terminal() {
			destination = "";
			mark = 0;
			road="";
		}

		public String getDestination() {
			return destination;
		}

		public void setDestination(String destination) {
			this.destination = destination;
		}

		public Integer getMark() {
			return mark;
		}

		public void setMark(Integer mark) {
			this.mark = mark;
		}

		public String getRoad() {
			return road;
		}

		public void setRoad(String road) {
			this.road = road;
		}
		
		public String toString() {
			StringBuilder sBuilder = new StringBuilder();
			sBuilder.append(destination).append(",").append(mark).append(",").append(road);
			return sBuilder.toString();
			
		}
	}
	
	public static class Graphy implements Serializable{
		private Integer weight;
		private String path;
		private ArrayList<Tuple2<String, Integer>> edge;
		private Integer flag;
		private Integer label;
		
		public Graphy() {
			// TODO Auto-generated constructor stub
			weight=-1;
			path="";
			edge= new ArrayList<Tuple2<String, Integer>>();
			flag=0;
			label=0;
		};
		
		public Integer getWeight() {
			return weight;
		}
		public void setWeight(Integer weight) {
			this.weight = weight;
		}
		public String getPath() {
			return path;
		}
		public void setPath(String path) {
			this.path = path;
		}
		public ArrayList<Tuple2<String, Integer>> getEdge() {
			return edge;
		}
		public void setEdge(ArrayList<Tuple2<String, Integer>> edge) {
			this.edge = edge;
		}
		public Integer getFlag() {
			return flag;
		}
		public void setFlag(Integer flag) {
			this.flag = flag;
		}
		public Integer getLabel() {
			return label;
		}
		public void setLabel(Integer label) {
			this.label = label;
		}
		
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("(").append(weight).append("|").append(path).append("|").append(edge.toString()).append("|").append(flag).append("|").append(label).append(")");
			return sb.toString();
		}
		
		
		
		
	}
	
	public static class tupp implements Serializable{
		String endnode;
		Integer weight;
		
		public tupp (String endnode,Integer weight) {
			this.endnode=endnode;
			this.weight=weight;
		}
		
		public String toString() {
			StringBuilder sBuilder = new StringBuilder();
			sBuilder.append("(").append(endnode).append(",").append(weight).append(")");
			return sBuilder.toString();
		}
	}
	
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("AssigTwo").setMaster("local");
		
		JavaSparkContext context = new JavaSparkContext(conf);
		
		JavaRDD<String> input = context.textFile(args[1]);
		
		String strc = args[0];
		
		JavaPairRDD<String, Tuple2<String, Integer>> step1 = input.mapToPair(new PairFunction<String, String, Tuple2<String,Integer>>() {

			@Override
			public Tuple2<String, Tuple2<String, Integer>> call(String line) throws Exception {
				// TODO Auto-generated method stub
				String[] parts = line.split(",");
				String stnode = parts[0];
				String endnode = parts[1];
				Integer weight = Integer.parseInt(parts[2]);
				Integer we =0;
				String info = "info";
				return new Tuple2<String, Tuple2<String,Integer>>(stnode, new Tuple2<>(endnode,weight));
			}
		});
		// step1 is to collect info from txt file and transform them into tuple, however, there is some node's info lost, for example, from the web photo N5
		// can be as a destination, so i use two rdd to join them to acquire N5's info.
		
		
		JavaPairRDD<String, Tuple2<String, Integer>> step2 = input.mapToPair(new PairFunction<String, String, Tuple2<String, Integer>>() {

			@Override
			public Tuple2<String, Tuple2<String, Integer>> call(String line) throws Exception {
				// TODO Auto-generated method stub
				String[] parts = line.split(",");
				String endnode = parts[1];
				Integer we =0;
				String info = "info";
				return new Tuple2<String, Tuple2<String,Integer>>(endnode, new Tuple2<>(info,we));
			}
		}).sortByKey();
		
		//JavaPairRDD<String, Tuple2<Optional<Tuple2<String, Integer>>, Optional<Tuple2<String, Integer>>>> step3 = step2.fullOuterJoin(step1);
		
		JavaPairRDD<String, Tuple2<Iterable<Tuple2<String, Integer>>, Iterable<Tuple2<String, Integer>>>> step3 = step2.cogroup(step1);
		
		
		JavaPairRDD<String, Graphy> step4 = step3.mapToPair(new PairFunction<Tuple2<String,Tuple2<Iterable<Tuple2<String,Integer>>,Iterable<Tuple2<String,Integer>>>>, String, Graphy>() {

			@Override
			public Tuple2<String, Graphy> call(
					Tuple2<String, Tuple2<Iterable<Tuple2<String, Integer>>, Iterable<Tuple2<String, Integer>>>> line)
					throws Exception {
				// TODO Auto-generated method stub
				ArrayList<Tuple2<String, Integer>> tt = new ArrayList<>();
				String stnode = line._1;
				if(line._2._2==null) {
					Graphy g = new Graphy();
					return new Tuple2<String, Graphy>(stnode,g);
				}
				else{
					for(Tuple2<String, Integer> t:line._2._2) {
						tt.add(t);
					}
					Graphy g = new Graphy();
					g.setEdge(tt);
					if(stnode.equals(strc)) {
						g.setWeight(0);
						g.setPath(strc);
						g.setLabel(1);
						return new Tuple2<String, Graphy>(stnode, g);
					}
					else {
						return new Tuple2<String, Graphy>(stnode, g);
					}
				}
			}
		});
		int c = (int) step4.count();
		int i=0;
		while(i<c) {
			step4 =  step4.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Graphy>, String, Graphy>() {
				
	
				@Override
				public Iterator<Tuple2<String, Graphy>> call(Tuple2<String, Graphy> line) throws Exception {
					ArrayList<Tuple2<String, Graphy>> aList = new ArrayList<Tuple2<String, Graphy>>();
					String stnode = line._1;
					Graphy g = line._2;
					String path = g.getPath();
					Integer oldwe = g.getWeight();
					if(g.getLabel()==1) {
						if(g.getWeight()>=0) {
							for(Tuple2<String, Integer> tu:g.getEdge()) {
								String detnode = tu._1;
								Integer we = tu._2;
								Graphy gg = new Graphy();
								gg.setFlag(1);
								gg.setPath(path+"-"+detnode);
								gg.setWeight(we+oldwe);
								aList.add(new Tuple2<String, Graphy>(detnode, gg));
							}
						}
					}
					aList.add(new Tuple2<String, Graphy>(stnode, g));
					return aList.iterator();
				}
			}).groupByKey().mapToPair(new PairFunction<Tuple2<String,Iterable<Graphy>>, String, Graphy>() {
	
				@Override
				public Tuple2<String, Graphy> call(Tuple2<String, Iterable<Graphy>> line) throws Exception {
					// TODO Auto-generated method stub
					String stnode = line._1;
					Graphy srcg = new Graphy();
					ArrayList<Tuple2<Integer, String>> isl = new ArrayList<>();
					
					for(Graphy gg:line._2) {
						if(gg.getFlag()==0) {
							srcg.setEdge(gg.getEdge());
							srcg.setWeight(gg.getWeight());
							srcg.setPath(gg.getPath());
						}
						if(gg.getEdge().size()==0 && gg.flag==1) {
							isl.add(new Tuple2<Integer, String>(gg.getWeight(), gg.getPath()));
						}
					}
					
					Integer mini = 999;
					String pp = "";
					if(isl.size()>0) {
						for(Tuple2<Integer, String> t:isl) {
							if(t._1 < mini) {
								mini=t._1;
								pp=t._2;
								
							}
						}
						if(srcg.getWeight()==-1) {
							srcg.setWeight(mini);
							srcg.setPath(pp);
						}
						
						if(mini<srcg.getWeight()) {
							srcg.setWeight(mini);
							srcg.setPath(pp);
						}
						srcg.setLabel(1);
	
					}
					
					return new Tuple2<String, Graphy>(stnode, srcg);
				}
			});
			i++;
			//step4.collect().forEach(System.out::println);
		}
		
		JavaPairRDD<String, Tuple2<Integer, String>> step5 = step4.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Graphy>, String, Tuple2<Integer,String>>() {

			@Override
			public Iterator<Tuple2<String, Tuple2<Integer, String>>> call(Tuple2<String, Graphy> line)
					throws Exception {
				// TODO Auto-generated method stub
				ArrayList<Tuple2<String,Tuple2<Integer,String>>> dwp = new ArrayList<>();
				String destnode = line._1;
				Graphy graphy = line._2;
				if(destnode.equals(strc)==false) {
					String pString= graphy.getPath();
					Integer weInteger = graphy.getWeight();
					dwp.add(new Tuple2<String, Tuple2<Integer,String>>(destnode, new Tuple2<>(weInteger, pString)));
					
				}
				return dwp.iterator();
			}
		});
		
		JavaRDD<Terminal> step6 = step5.map(new Function<Tuple2<String,Tuple2<Integer,String>>, Terminal>() {

			@Override
			public Terminal call(Tuple2<String, Tuple2<Integer, String>> line) throws Exception {
				// TODO Auto-generated method stub
				String deString = line._1;
				Integer we = line._2._1;
				String pa = line._2._2;
				Terminal terminal = new Terminal();
				terminal.setDestination(deString);
				terminal.setMark(we);
				terminal.setRoad(pa);
				return terminal;
			}
		}).sortBy(new Function<Terminal, Integer>() {

			@Override
			public Integer call(Terminal line) throws Exception {
				// TODO Auto-generated method stub
				if(line.getMark()==-1) {
					return Integer.MAX_VALUE;
				}
				return line.getMark();
			}
		}, true, 1);
		
		step6.saveAsTextFile(args[2]);
		
		
		
		
		//.collect().forEach(System.out::println);
//		JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> step4 = step3.mapToPair(new PairFunction<Tuple2<String,Tuple2<Optional<Tuple2<String,Integer>>,Optional<Tuple2<String,Integer>>>>, String, Tuple2<String,Integer>>() {
//
//			@Override
//			public Tuple2<String, Tuple2<String, Integer>> call(
//					Tuple2<String, Tuple2<Optional<Tuple2<String, Integer>>, Optional<Tuple2<String, Integer>>>> line)
//					throws Exception {
//				// TODO Auto-generated method stub
//				String stnode = line._1;
//				if(line._2._1.isPresent()) {
//					Tuple2<String, Integer> tt = new Tuple2<String, Integer>(line._2._1.get()._1,line._2._1.get()._2);
//					return new Tuple2<String, Tuple2<String,Integer>>(stnode, tt);
//					}
//				else {
//					return new Tuple2<String, Tuple2<String,Integer>>(stnode, new Tuple2<>("",0));
//				}
//				
//			}
//		}).groupByKey();
//		
//		step4.collect().forEach(System.out::println);
//		JavaPairRDD<String, Graphy> step2 = step1.flatMapToPair(new PairFunction<Tuple2<String,Iterable<Tuple2<String,Integer>>>, String, Graphy>() {
//
//			@Override
//			public Tuple2<String, Graphy> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> input)
//					throws Exception {
//				// TODO Auto-generated method stub
//				ArrayList<Tuple2<String, Integer>> asArrayList = new ArrayList<>();
//				String src = input._1;
//				for(Tuple2<String, Integer> t:input._2) {
//					asArrayList.add(t);
//				}
	//				Graphy g = new Graphy();
	//				g.setEdge(asArrayList);
	//				if(src.equals(strc)) {
	//					g.setWeight(0);
	//				}
	//				return new Tuple2<String,Graphy>(src, g);
//			}
//		});
//		step2.collect().forEach(System.out::println);
//		
		
		
	}

}
