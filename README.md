# Graph Processing
## Info

Academic project that computes varying distributions and summary network statistics for the Slashdot-Zoo dataset. (available here http://konect.uni-koblenz.de/networks/slashdot-zoo)
Based on Gelly, the graph-processing API of Apache Flink.

### Main tasks
- Compute and plot  the in-degree and out-degree distributions (InDegreeDistribution)
- Compute and plot the out-degree distributions for both the friend and foe nodes, individually (SignedOutDegreeDistribution)
- Compute and report both the average degree and the maximum degree of all vertices (DegreeDistribution)
- Identify and report the vertex with the most friends and the most foes (VertexQuery)
- Calculate and report the average friend to foe ratio (AverageFriendFoeRatio)

It includes also a Python Notebook plotting the degree distributions.
