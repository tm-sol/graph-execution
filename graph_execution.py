from collections import deque
from concurrent.futures import ThreadPoolExecutor
import matplotlib
matplotlib.use('TkAgg')  # for OSX graph drawing
import networkx as nx
import pylab as p
import sys
import time
from timeit import Timer


class Requirement(object):
    """ Abstract base class for actions that may depend on other actions """

    def __init__(self, label, account):
        """ Label is for visualising this as part of a graph, account is what is targetted by the Requirement """
        self.label = label
        self.account = account

    def sync(self):
        """ Perform the action itself - just a dummy implementation that sleeps for 1/2 a second """
        print("{} started".format(self))
        time.sleep(0.5)
        print("{} done".format(self))
        return "Result of {}".format(self)

    def __repr__(self):
        return self.label

    def __str__(self):
        return self.label


class CreateAccount(Requirement):

    def get_dependencies(self, requirements):
        return []


class AdminAccess(Requirement):

    def get_dependencies(self, requirements):
        return [req for req in requirements if isinstance(req, CreateAccount) and req.account == self.account]


class CloudTrailSNSTopic(Requirement):

    def get_dependencies(self, requirements):
        return [req for req in requirements if (isinstance(req, CreateAccount) and req.account == 'syslog')
                or (isinstance(req, AdminAccess) and req.account == self.account)]


class CloudTrailTrail(Requirement):

    def get_dependencies(self, requirements):
        return [req for req in requirements if (isinstance(req, S3Bucket) and req.account == 'syslog')
                or (isinstance(req, CloudTrailSNSTopic) and req.account == self.account)]


class S3Bucket(Requirement):

    def get_dependencies(self, requirements):
        return [req for req in requirements if (isinstance(req, CreateAccount) and req.account != self.account)
                or (isinstance(req, AdminAccess) and req.account == self.account)]


class SQSQueue(Requirement):

    def get_dependencies(self, requirements):
        return [req for req in requirements if (isinstance(req, CloudTrailSNSTopic) and req.account != self.account)
                or (isinstance(req, AdminAccess) and req.account == self.account)]


def build_graph(num_accounts):
    """ Build a DAG of multiple instances of the various Requirement subclasses """

    all_requirements = []

    # Syslog account requirements
    syslog_account = 'syslog'
    all_requirements.append(CreateAccount('Create [Syslog]', syslog_account))
    all_requirements.append(AdminAccess('Admin Access [Syslog]', syslog_account))
    all_requirements.append(S3Bucket('S3 Bucket [Syslog]', syslog_account))
    all_requirements.append(SQSQueue('SQS Queue [Syslog]', syslog_account))

    # PDU account(s) requirements
    for n in range(1, num_accounts+1):
        pdu_account = "PDU{}".format(n)
        all_requirements.append(CreateAccount("Create [{}]".format(pdu_account), pdu_account))
        all_requirements.append(AdminAccess("Admin Access [{}]".format(pdu_account), pdu_account))
        all_requirements.append(CloudTrailSNSTopic("CloudTrail SNS [{}]".format(pdu_account), pdu_account))
        all_requirements.append(CloudTrailTrail("CloudTrail Trail [{}]".format(pdu_account), pdu_account))

    # Build graph based on each requirement's dependencies
    g = nx.DiGraph()
    for req in all_requirements:
        print("Adding node '{}'".format(req))
        g.add_node(req)
        dependencies = req.get_dependencies(all_requirements)
        for dep in dependencies:
            print("Adding edge from '{}' to '{}'".format(dep, req))
            g.add_edge(dep, req)
    return g


def topological_sort(dag, breadth_first=True):
    """ Breadth-first toplogical sort of DAG: https://www.quora.com/Can-topological-sorting-be-done-using-BFS """

    def init_inbound_counts(nodes, edges):
        """ Produce an initial dict that maps the nodes to the number of their inbound-connections """
        inbound_counts = {}
        for node in nodes:
            inbound_counts[node] = 0
        for e in edges:
            inbound_counts[e[1]] = inbound_counts[e[1]] + 1
        return inbound_counts

    def init_ready_queue(inbound_counts, nodes):
        """ Produce an initial ready that contains only those nodes with no inbound edges """
        ready = [n for n in nodes if inbound_counts[n] == 0]
        return deque(ready)  # to get popleft()

    def reduce_inbound_connections(inbound_counts, nodes):
        """ Update the inbound_counts to reflect the removal of one inbound edge from each node in nodes. 
            Return all nodes that (now) have no inbound edges at all """
        nodes_without_inbound = []
        for node in nodes:
            inbound_counts[node] = inbound_counts[node] - 1
            if inbound_counts[node] == 0:
                nodes_without_inbound.append(node)
        return nodes_without_inbound

    sorted_nodes = []
    inbound_counts = init_inbound_counts(dag.nodes(), dag.edges())
    ready_queue = init_ready_queue(inbound_counts, dag.nodes())
    while not len(ready_queue) == 0:
        if breadth_first:
            current_node = ready_queue.popleft()  # leave new 'ready' nodes till last
        else:
            current_node = ready_queue.pop()  # eagerly do new 'ready' nodes first
        sorted_nodes.append(current_node)
        new_ready_nodes = reduce_inbound_connections(inbound_counts, dag.neighbors(current_node))
        ready_queue.extend(new_ready_nodes)
    return sorted_nodes


def print_order(dag):
    print("Depth-first toplogical sort: {}".format(", ".join([str(node) for node in nx.topological_sort(dag)])))
    print("Depth-first toplogical sort using local function: {}".format(
        ", ".join([str(node) for node in topological_sort(dag, False)])))
    print("Breadth-first toplogical sort: {}".format(", ".join([str(node) for node in topological_sort(dag, True)])))


def run_serially(sorted_nodes, fun):
    """ Execute the 'fun' function on every node in the graph in the supplied order, with each node being targetted
        in serial (one-by-one) """
    results = {}
    for node in sorted_nodes:
        result = fun(node)
        results[node] = result
    return results


def run_concurrently(sorted_nodes, fun):
    """ Execute the 'fun' function on every node in the graph in the supplied order (assumed to be in order of dependency
        i.e. a topologically-sorted DAG) with execution of multiple nodes run concurrently whenever this is possible 
        (i.e. without starting a node while its predecessors are running) """

    results = {}
    in_progress = {}

    def register_execution(in_progress, future, node):
        """ Register a future in the 'in_progress' dict, with the value being the node of the graph """
        in_progress[future] = node

    def handle_done(future):
        """ Callback whenever a future execution of a node has completed. Collects the results and removes the
            future from the 'in_progress' dict """
        results[in_progress[future]] = future.result()
        del in_progress[future]

    def wait_on_predecessors(dag, node, in_progress):
        """ Wait for any predecessors of node in the graph to complete execution before returning """
        while True:
            all_predecessors = dag.predecessors(node)
            in_progress_predecessors = list(set(all_predecessors) & set(in_progress.values()))
            if not in_progress_predecessors:
                print("all dependencies of {} completed".format(node))
                break
            print("delaying execution of {} until {} complete(s)..".format(node, in_progress_predecessors))
            time.sleep(0.1)

    with ThreadPoolExecutor(max_workers=4) as executor:
        for node in sorted_nodes:
            wait_on_predecessors(dag, node, in_progress)
            future = executor.submit(fun, node)
            register_execution(in_progress, future, node)
            future.add_done_callback(handle_done)  # if already done, the callback is called immediately
    return results


def print_graph(dag, image_path, graph_path):
    """ Print the graph to the specified paths as a) a PNG image and b) a graphml file"""
    for node in dag.nodes():
        dag.node[node]['label'] = node.label
    nx.write_graphml(dag, graph_path)
    pos = nx.random_layout(dag)
    nx.draw_networkx(dag, ax=None, width=3, pos=pos)
    p.savefig(image_path)

# If an arg is supplied it is assumed to be the number of PDU accounts to 'create' when instantiating Requirements
num_accounts = 1
if len(sys.argv) > 1:
    num_accounts = int(sys.argv[1])

print("Building graph..")
dag = build_graph(num_accounts)
print("Building graph - done")

print("------")

print_graph(dag, './graph.png', './graph.graphml')
print("Printed graph to './graph.png'")

print("------")

print("Topological graph orders: ")
print_order(dag)

print("------")

print("Executing topologically-sorted graph in serial..")
depth_first_sorted_nodes = nx.topological_sort(dag)
serial_timer = Timer(lambda: print(run_serially(depth_first_sorted_nodes, lambda req: req.sync())))
serial_elapsed = serial_timer.timeit(number=1)
print("Elapsed time for serial execution {:0.2f} seconds".format(serial_elapsed))

print("------")

print("Executing (breadth-first) topologically-sorted graph concurrently ..")
breadth_first_sorted_nodes = topological_sort(dag, True)
concurrent_progress_timer = Timer(lambda: print(run_concurrently(breadth_first_sorted_nodes, lambda req: req.sync())))
concurrent_progress_elapsed = concurrent_progress_timer.timeit(number=1)
print("Elapsed time for concurrent execution {:0.2f} seconds".format(concurrent_progress_elapsed))

print("------")
difference_secs = serial_elapsed - concurrent_progress_elapsed
difference_percent = 100 * (difference_secs / serial_elapsed)
print(
    "Reduction in time for breadth-first concurrent execution over serial execution: {:0.2f} seconds ({:0.2f}%)".format(
        difference_secs, difference_percent))
