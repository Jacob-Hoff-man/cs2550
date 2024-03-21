# this code uses networx lib.
import networkx as nx


class DeadlockDetector:
    def __init__(self):
        self._wait_for_graph = nx.DiGraph()  # create a new digraph 

    def wait_for_graph(self):
        return self._wait_for_graph

    # creates the association between two Txs, then check rightawy for cycles.
    def wait_for(self, waiting_transaction_id, waiting_for_transaction_id):
        if not(self._wait_for_graph.has_node(waiting_transaction_id)):
            self._wait_for_graph.add_node(waiting_transaction_id)
        if not(self._wait_for_graph.has_node(waiting_for_transaction_id)):
            self._wait_for_graph.add_node(waiting_for_transaction_id)

        self._wait_for_graph.add_edge(waiting_transaction_id, waiting_for_transaction_id)
        deadlock_cycle = self.find_deadlock_cycle()
        if deadlock_cycle is not None:
            self._wait_for_graph.remove_edge(waiting_transaction_id, waiting_for_transaction_id)
            return deadlock_cycle
        return None

    # call after a Tx is done, will delete Tx and its edges
    def transaction_ended(self, ended_transaction_id):
        if self._wait_for_graph.has_node(ended_transaction_id):
            # should remove all the connected edges to the ended_transaction_id
            self._wait_for_graph.remove_node(ended_transaction_id)

    # if the digraph is cyclic, thenretun the cycle, otherwise return NONE
    def find_deadlock_cycle(self):
        try:
            cycle = nx.find_cycle(self._wait_for_graph, orientation='original')
            return cycle
        except nx.NetworkXNoCycle:
            return None