package Dijkstra;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
public class Dijkstra {
    public static class Graph {

        private Set<Node> nodes = new HashSet<Node>();

        public void addNode(Node nodeA) {
            nodes.add(nodeA);
        }

        public Set<Node> getNodes() {
            return nodes;
        }

        public void setNodes(Set<Node> nodes) {
            this.nodes = nodes;
        }

        @Override
        public String toString() {
            return "Graph [nodes=" + nodes + "]\n";
        }

        public Node findNodeByName(String name) {
            for (Node node : nodes) {
                if (node.getName().equals(name)) {
                    return node;
                }
            }
            return null; 
        }

    }
    public static class Node {

        private String name;

        private List<Node> shortestPath = new LinkedList<Node>();

        private Integer distance = Integer.MAX_VALUE;

        Map<Node, Integer> adjacentNodes = new HashMap<Node, Integer>();

        public void addDestination(Node destination, int distance) {
            adjacentNodes.put(destination, distance);
        }

        public Node(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

       

        public List<Node> getShortestPath() {
            return shortestPath;
        }

        public void setShortestPath(List<Node> shortestPath) {
            this.shortestPath = shortestPath;
        }

        public Integer getDistance() {
            return distance;
        }

        public void setDistance(Integer distance) {
            this.distance = distance;
        }

        public Map<Node, Integer> getAdjacentNodes() {
            return adjacentNodes;
        }

       
        @Override
        public String toString() {
            StringBuilder pathStringBuilder = new StringBuilder();
            for (Node node : shortestPath) {
                pathStringBuilder.append(node.getName()).append(" -> ");
            }
            pathStringBuilder.append(name);
            return "Node [name=" + name + " ,shortestPath=" + pathStringBuilder.toString() + " ,distance=" + distance + "]";
        }
    }

    public static Graph pathPlusCourt(Graph graph, Node source) {
       
    	if(source!=null)
    	{
        	source.setDistance(0);

        Set<Node> newNodeList = new HashSet<Node>();
        Set<Node> nodeList = new HashSet<Node>();

        nodeList.add(source);

        while (!nodeList.isEmpty()) {
            Node courtNode = getcourtNodeDistance(nodeList);
            nodeList.remove(courtNode);

            for (Entry<Node, Integer> adja : courtNode.getAdjacentNodes().entrySet()) {
                Node adjacentNode = adja.getKey();
                Integer distance = adja.getValue();

                if (!newNodeList.contains(adjacentNode)) {
                    minimumDistance(adjacentNode, distance, courtNode);
                    nodeList.add(adjacentNode);
                }
            }

            newNodeList.add(courtNode);
          }
    	}

        return graph;
    }

    public static Node getcourtNodeDistance(Set<Node> nodeList) {
        Node courtNodeDistance = null;
        int courtDistance = Integer.MAX_VALUE;
        for (Node node : nodeList) {
            int nodeDistance = node.getDistance();
            if (nodeDistance < courtDistance) {
                courtDistance = nodeDistance;
                courtNodeDistance = node;
            }
        }
        return courtNodeDistance;
    }

    public static void minimumDistance(Node NodeAcompare, Integer distance, Node sourceNode) {
        Integer sourceDistance = sourceNode.getDistance();
        if (sourceDistance + distance < NodeAcompare.getDistance()) {
            NodeAcompare.setDistance(sourceDistance + distance);
            List<Node> shortestPath = new LinkedList<Node>(sourceNode.getShortestPath());
            shortestPath.add(sourceNode);
            NodeAcompare.setShortestPath(shortestPath);
        }
    }
/*---------------------------------------MAPPER------------------------------------------------*/
    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] tokens = value.toString().split(";");

            String sourceNode = tokens[0];
            String destinationNode = tokens[1];
            Integer distance = Integer.parseInt(tokens[2]);

            context.write(new Text(sourceNode), new Text(destinationNode + ";" + distance));
        }
    }

    static Graph graph = new Graph();
  /*-------------------------------------REDUCER--------------------------------------------------*/

    public static class ReduceClass extends Reducer<Text, Text, Text, Text>
      {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Node sourceNode = graph.findNodeByName(key.toString());
            if (sourceNode == null) {
                sourceNode = new Node(key.toString());
                graph.addNode(sourceNode);
            }
            
            for (Text value : values) {
                String[] parts = value.toString().split(";");
                String destinationName = parts[0];
                Integer distance = Integer.parseInt(parts[1]);

                Node destinationNode = graph.findNodeByName(destinationName);

                if (destinationNode == null) {
                    destinationNode = new Node(destinationName);
                    graph.addNode(destinationNode);
                }

                sourceNode.addDestination(destinationNode, distance);
            }
            
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
        	
        			
        	     for(Node src:graph.getNodes())
        				{
        					pathPlusCourt(graph, src);
        				
        					context.write(new Text("\nPathPlusCourteDuSource  "+src.getName()+" ==>"), new Text(graph.getNodes().toString()));
		        			
		        			/* reset de la graph */
        					for (Node node : graph.getNodes()) {
		        			    node.setDistance(Integer.MAX_VALUE);
		        			    node.setShortestPath(new LinkedList<Node>());
		        			}
        				}
             	
        }
    }
  /*--------------------------------------MAIN---------------------------------------------------------*/
    public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Dijkstra");
		job.setJarByClass(Dijkstra.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(1); 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[0]));
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
