from conn import Neo4jConnection

# Connection to the database 
conn = Neo4jConnection(uri="bolt://localhost:7687", 
                       user="neo4j", pwd="1234")


conn.query("CREATE OR REPLACE DATABASE neo4j")

'''The Cora dataset consists of 2708 scientific publications 
classified into one of seven classes. The citation network 
consists of 5429 links. Each publication in the dataset is 
described by a 0/1-valued word vector indicating the 
absence/presence of the corresponding word from the dictionary. 
The dictionary consists of 1433 unique words.'''

# Create the citation graph from CSV files
# Read the CSV file containing nodes information line by line 
# and add each node to the graph with label Paper and properties id and class
query_string = '''
USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM
'https://raw.githubusercontent.com/ngshya/datasets/master/cora/cora_content.csv'
AS line FIELDTERMINATOR ','
CREATE (:Paper {id: line.paper_id, class: line.label})
'''
conn.query(query_string, db='neo4j')



# Same with edges information
query_string = '''
USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM
'https://raw.githubusercontent.com/ngshya/datasets/master/cora/cora_cites.csv'
AS line FIELDTERMINATOR ','
MATCH (citing_paper:Paper {id: line.citing_paper_id}),(cited_paper:Paper {id: line.cited_paper_id})
CREATE (citing_paper)-[:CITES]->(cited_paper)
'''
conn.query(query_string, db='neo4j')


# Ask which are the classes of papers in the network
query_string = '''
MATCH (p:Paper)
RETURN DISTINCT p.class
ORDER BY p.class
'''
conn.query(query_string, db='neo4j')


# List of most cited papers
query_string = '''
MATCH ()-->(p:Paper) 
RETURN id(p), count(*) as indegree 
ORDER BY indegree DESC LIMIT 10
'''
conn.query(query_string, db='neo4j')


conn.close()

