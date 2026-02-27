from neo4j import GraphDatabase

class Interface:
    def __init__(self, db_uri, db_user, db_password):
        self._db_driver = GraphDatabase.driver(db_uri, auth=(db_user, db_password), encrypted=False)
        self._db_driver.verify_connectivity()

    def close(self):
        """Close the connection to the Neo4j database."""
        self._db_driver.close()

    def create_graph(self):
        """
        Creates a graph projection with 'distance' as a relationship property.
        """
        with self._db_driver.session() as db_session:
            # Create the graph projection with 'distance'
            query = """
            CALL gds.graph.project(
                'myGraph', 
                'Location', 
                'TRIP', 
                { relationshipProperties: ['distance'] }
            )
            """
            db_session.run(query)

    def bfs(self, start_location, target_locations):
        if not isinstance(target_locations, list):
            target_locations = [target_locations]

        # Create the graph projection
        self.create_graph()  
        
        with self._db_driver.session() as db_session:            
            target_location = target_locations[0]

            # BFS Algorithm using Neo4j GDS
            query = """
            MATCH (source:Location {name: $start_location}), (target:Location {name: $target_location})
            WITH source, [target] AS targetNodes
            CALL gds.bfs.stream('myGraph', {
                sourceNode: source,
                targetNodes: targetNodes
            })
            YIELD path
            RETURN [node IN nodes(path) | {name: node.name}] AS path
            """
            params = {"start_location": start_location, "target_location": target_location}
            result = db_session.run(query, **params)

            return result.data()

    def pagerank(self, max_cycles=20, weight_attr="distance"):
        # Create the graph projection 
        self.create_graph()

        with self._db_driver.session() as db_session:
            query = """
            CALL gds.pageRank.stream('myGraph', {
                maxIterations: $max_cycles,
                dampingFactor: 0.85,
                relationshipWeightProperty: $weight_attr
            })
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).name AS node, score
            ORDER BY score DESC
            """
            params = {"max_cycles": max_cycles, "weight_attr": weight_attr}

            result = db_session.run(query, **params)

            rankings = result.data()

            if not rankings:
                return None, None  

            # Sort the rankings by score (in descending order)
            sorted_rankings = sorted(rankings, key=lambda x: x['score'], reverse=True)

            # Extract the first and last node along with their scores
            top_node = sorted_rankings[0]
            bottom_node = sorted_rankings[-1]

            return [{"name": top_node['node'], "score": top_node['score']}, 
                    {"name": bottom_node['node'], "score": bottom_node['score']}]