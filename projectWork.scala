import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object Tri {

  def main(args: Array[String]): Unit = { // Entry point for the application
    val sc = getSC() // Get the SparkContext
    val edgeList = toyGraph(sc) // Load the toy graph or replace with `getFB(sc)` for real data
    val triangleCount = countTriangles(edgeList) // Count triangles in the graph

    // Save the result to HDFS
    sc.parallelize(List(triangleCount)).saveAsTextFile("NumberOfTriangles")
  }

   //redudant doesnt need explination, similar to sparklab 
  def getSC(): SparkContext = {
    val conf = new SparkConf().setAppName("Triangle Counting").setMaster("local[*]")
    new SparkContext(conf)
  }

   def getFB(sc: SparkContext): RDD[(String, String)] = {
    /*
     * Purpose: Load the Facebook dataset from the specified path and convert it into an RDD of node pairs.
     
     * Steps:
     * 1. Read the dataset from the file using `sc.textFile`.
     * 2. Trim each line to remove any extra spaces.
     * 3. Split the line by a single space to extract pairs of node IDs.
     * 4. Filter out any malformed or invalid lines (lines with fewer than 2 elements).
     * 5. Map each valid pair into a tuple of strings.
     */
    sc.textFile("/datasets/facebook")
      .map(_.trim) // Remove any extra spaces
      .map(_.split(" ")) //split spaces
      .filter(_.length == 2) // make sure only valid pairs
      .map(parts => (parts(0), parts(1))) 
  }

  def getFB(sc: SparkContext): RDD[(String, String)] = {
    sc.textFile("/datasets/facebook")
      .map(_.trim) // Remove extra spaces
      .map(_.split(" ")) // Split by single space
      .filter(_.length == 2) // Only valid pairs
      .map(parts => (parts(0), parts(1))) // Create tuples
  }

    def makeRedundant(edgeList: RDD[(String, String)]): RDD[(String, String)] = {
    /*
     * Purpose: Make the edge list redundant to ensure the graph is undirected.
     *
     * Steps:
     * 1. Use flatMap to emit both (a, b) and (b, a) for each edge.
     * 2. then `distinct` to remove duplicate entries, ensuring a clean, redundant edge list.
     */
    edgeList.flatMap { case (a, b) => Seq((a, b), (b, a)) }.distinct()
  }

 
  def noSelfEdges(edgeList: RDD[(String, String)]): RDD[(String, String)] = {
    /*
     * Purpose: Remove self-loops from the edge list.
     *
     * Steps:
     * 1. Use `filter` to exclude any tuples where the source node and destination node are the same.
     */
    edgeList.filter { case (a, b) => a != b }
  }

 


  def friendsOfFriends(edgeRDD: RDD[(String, String)]): RDD[(String, (String, String))] = {
    /*
     * Purpose: Identify pairs of nodes that share a common friend.
     *
     * Input: An RDD of edges (nodeA, nodeB), where nodeA and nodeB represent connected nodes.
     *
     * Output: An RDD of the form (commonNode, (neighbor1, neighbor2)), indicating that neighbor1
     *         and neighbor2 are both connected to the commonNode.
     *
     * Steps:
     * 1. Group edges by their first node (commonNode) to collect all connected neighbors.
     * 2. Iterate over all combinations of neighbors for each node using `flatMap`.
     * 3. Ensure each pair (neighbor1, neighbor2) is ordered to avoid duplicate pairs.
     * 4. Emit (commonNode, (neighbor1, neighbor2)) for each valid pair.
     */
    edgeRDD.groupByKey() // Group edges by the first node
      .flatMap { case (commonNode, neighborSet) =>
        val uniqueNeighbors = neighborSet.toSet // Remove duplicates
        uniqueNeighbors.toSeq.combinations(2) // Generate all unique pairs
          .map { case Seq(neighbor1, neighbor2) =>
            (commonNode, (neighbor1, neighbor2)) // Emit the pair with the commonNode
          }
      }
  }



   
  def journeyHome(edgeList: RDD[(String, String)],
                  twoPaths: RDD[(String, (String, String))]): RDD[((String, String), String)] = {
    /*
     * Purpose: Verify which pairs of friends are directly connected to complete a triangle.
     *
     * Input:
     *   edgeList representing connections in the graph.
     *   An RDD of the form (commonNode, (friend1, friend2)), representing paths of length 2.
     * Output: An RDD of the form ((friend1, friend2), commonNode), where friend1 and friend2 form a triangle
     *         with the commonNode.
     *
     * Steps:
     * 1. Reverse the edge list to prepare for joining with the twoPaths RDD.
     * 2. Join the `twoPaths` RDD with the reversed edge list based on the intermediate node.
     * 3. Map the result to emit pairs of friends (friend1, friend2) and their common node.
     */
    val reversedEdges = edgeList.map { case (a, b) => (b, a) } // Flip edge direction

    // Join edges with two-paths based on the intermediate node
    twoPaths.join(reversedEdges)
      .map { case (_, ((friend1, friend2), _)) =>
        ((friend1, friend2), _) // Emit pairs of friends that complete a triangle
      }
  }

  def toyGraph(sc: SparkContext): RDD[(String, String)] = {
    /*
     * Purpose: Create a small example graph for testing the triangle counting algorithm.
     *
     * Output: An RDD of edges representing the toy graph.
     *
     * Graph Structure:
     *   1 ----- 2
     *   | \     |
     *   |   \   |
     *   |     \ |
     *   4-------3 ------ 5
     *
     * The graph contains 2 triangles: (1, 2, 3) and (1, 3, 4).
     */
    val edges = List(
      ("1", "2"), ("2", "1"),
      ("2", "3"), ("3", "2"),
      ("1", "3"), ("3", "1"),
      ("1", "4"), ("4", "1"),
      ("4", "3"), ("3", "4"),
      ("3", "5"), ("5", "3"),
      ("1", "1") // Self-loop (should be removed later)
    )
    sc.parallelize(edges)
  }
  def countTriangles(edgeList: RDD[(String, String)]): Long = {
    /*
     * Purpose: Count the number of triangles in the graph.
     *
     * Steps:
     * 1. Remove self-edges from the graph.
     * 2. Make the edge list redundant to ensure undirected representation.
     * 3. Find all friends-of-friends pairs using the redundant edge list.
     * 4. Verify which friend pairs form a triangle.
     * 5. Divide the final count by 3 to account for overcounting.
     */

    // Step 1: Remove self-edges
    val filteredEdges = noSelfEdges(edgeList)

    // Step 2: Make the edge list redundant
    val undirectedEdges = makeRedundant(filteredEdges)

    // Step 3: Find friends-of-friends pairs
    val friendsOfFriendsPairs = friendsOfFriends(undirectedEdges)

    // Step 4: Verify triangles by joining
    val triangleEdges = journeyHome(undirectedEdges, friendsOfFriendsPairs)

    // Step 5: Divide the count by 3 since each triangle is counted three times
    triangleEdges.count() / 3
  }

}


 

