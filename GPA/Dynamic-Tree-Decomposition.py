class TreeDecomposition:
    def __init__(self, graph):
        self.graph = graph
        self.tree_decomposition = None

    def get_tree_decomposition(self):
        if self.tree_decomposition is None:
            self.tree_decomposition = self.compute_tree_decomposition()
        return self.tree_decomposition

    def compute_tree_decomposition(self):
        # Step 1: Compute the bags for each node in the tree decomposition
        bags = {}
        for node in self.graph.nodes():
            bags[node] = {node}
            for neighbor in self.graph.neighbors(node):
                bags[node].update({neighbor})

        # Step 2: Compute the tree decomposition using dynamic programming
        n = self.graph.number_of_nodes()
        tree_decomposition = {frozenset(): (None, set())}
        for k in range(n):
            for subset in combinations(range(n), k):
                subgraph = self.graph.subgraph(subset)
                for node in subset:
                    for bag, (root, children) in tree_decomposition.items():
                        if bag.issubset(subset) and node not in bag:
                            new_bag = bag.union({node})
                            new_root = root if root is not None else node
                            new_children = children.union({frozenset(new_bag)})
                            if new_bag not in tree_decomposition or len(new_bag) < len(tree_decomposition[new_bag][0]):
                                tree_decomposition[new_bag] = (new_root, new_children)

        return tree_decomposition

    def get_tree_width(self):
        tree_decomposition = self.get_tree_decomposition()
        return max(len(bag) - 1 for bag in tree_decomposition.keys())
