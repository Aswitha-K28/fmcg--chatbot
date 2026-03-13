from neo4j import GraphDatabase

uri= "neo4j+s://46991046.databases.neo4j.io"
user = "46991046"
password = "bT3ebt7nA6Cbz3CofkpdMfKBgVxUW7vJmpJOKverdu8"

driver = GraphDatabase.driver(uri, auth=(user, password))
driver.verify_connectivity()
print("Connected successfully")
driver.close()