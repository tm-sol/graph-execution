# Graph Execution

This is a POC that establishes a mechanism by which multiple 'requirements' (things that need to be true in a cloud environment) can express their dependency on other Requirement types, and those dependencies be honoured in the execution order.

## Notes

* To run this POC just execute the following and watch the output:
  
  ```
  python graph_execution.py 
  ```
  
* The graph library used is [networkx](http://networkx.readthedocs.io/en/networkx-1.10/index.html).
* The graph type is [DiGraph](http://networkx.readthedocs.io/en/networkx-1.10/tutorial/tutorial.html#directed-graphs) (i.e. a DAG) and the execution transforms the graph into a linear sequence of nodes using a [topological sort](http://www.geeksforgeeks.org/topological-sorting/).
* The Requirement classes express a chain of dependencies as follows:
 
   ```
           RequirementA
           /         \
    RequirementB  RequirementD 
          \           |
           \       RequirementE
            \        /
           RequirementC
   ```
   
* The actual DAG is more complex (different As pointing to different Bs and Ds etc.) as can be seen from the image written by the POC to graph.png. The complexity arises because:
	* The code creates multiple instances of each Requirement type. 
	* Some of the dependencies are between Requirement instances only if they share a reference to the same account. This reflects what would often be case - e.g. account Requirements that depend upon that same account's 'exists' Requirement, but not every other 'exists' Requirement.
* The execution of the graph (calling Requirement.diff() on each Requirement instance) is done twice: once in serial, where each Requirement must finish before the next is started, and once concurrently, where as many Requirements as possible (to a maximum of 4) are started as long as none of their predecessors is running.
* The [NetworkX topological sort implementation](http://networkx.readthedocs.io/en/networkx-1.10/reference/generated/networkx.algorithms.dag.topological_sort.html) is depth-first, which is correct and perfectly fine if you execute serially. However, when you are executing tasks concurrently you want to prioritise clearing nodes at a given depth before going deeper into the graph, which makes depth-first less than optimal. For this reason the POC adds its own breadth-first topological sort function ```topological_sort_bfs(Graph)``` based on the answers to [this question](https://www.quora.com/Can-topological-sorting-be-done-using-BFS) on quora.com. Both sort orders are printed out to the console when the POC is run.
* The impact of serial and concurrent (breadth-first) execution times can be seen in the output of the script.   

