import os
import csv
import shutil
import pyspark
import pandas as pd
import networkx as nx
from pyspark.sql import SparkSession, Row
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.sql import functions as F
from graphframes import GraphFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from networkx.exception import NetworkXError
import time
from datetime import datetime


# function
def graphframe_to_networkx(graphframe):
    # Convert vertices and edges to Pandas DataFrame
    vertices_pd = graphframe.vertices.toPandas()
    edges_pd = graphframe.edges.toPandas()

    # Create an empty NetworkX graph
    G = nx.DiGraph()  # Use nx.Graph() for an undirected graph

    # Add nodes with attributes from vertices DataFrame
    for index, row in vertices_pd.iterrows():
        G.add_node(row['id'], **row.to_dict())

    # Add edges from edges DataFrame
    for index, row in edges_pd.iterrows():
        G.add_edge(row['src'], row['dst'])

    return G


def modified_pagerank(G, alpha=0.85, personalization=None, 
                    max_iter=100, tol=1.0e-6, nstart=None, weight='weight', 
                    dangling=None):
    if len(G) == 0: 
        return {} 

    if not G.is_directed(): 
        D = G.to_directed() 
    else: 
        D = G 

    # Create a copy in (right) stochastic form 
    W = nx.stochastic_graph(D, weight=weight) 
    N = W.number_of_nodes() 

    # Choose fixed starting vector if not given 
    if nstart is None: 
        x = dict.fromkeys(W, 1.0 / N) 
    else: 
        # Normalized nstart vector 
        s = float(sum(nstart.values())) 
        x = dict((k, v / s) for k, v in nstart.items()) 

    if personalization is None: 
        # Assign uniform personalization vector if not given 
        p = dict.fromkeys(W, 1.0 / N) 
    else: 
        missing = set(G) - set(personalization) 
        if missing: 
            raise NetworkXError('Personalization dictionary must have a value for every node. Missing nodes %s' % missing) 
        s = float(sum(personalization.values())) 
        p = dict((k, v / s) for k, v in personalization.items()) 

    if dangling is None: 
        # Use personalization vector if dangling vector not specified 
        dangling_weights = p 
    else: 
        missing = set(G) - set(dangling) 
        if missing: 
            raise NetworkXError('Dangling node dictionary must have a value for every node. Missing nodes %s' % missing) 
        s = float(sum(dangling.values())) 
        dangling_weights = dict((k, v/s) for k, v in dangling.items()) 
    dangling_nodes = [n for n in W if W.out_degree(n, weight=weight) == 0.0] 

    # power iteration: make up to max_iter iterations 
    for _ in range(max_iter): 
        xlast = x 
        x = dict.fromkeys(xlast.keys(), 0) 
        danglesum = alpha * sum(xlast[n] for n in dangling_nodes) 
        for n in x: 
            for nbr in W[n]: 
                # Include self-loop in the calculation
                if nbr == n: 
                    x[n] += alpha * xlast[n] * W[n][nbr].get(weight, 1) 
                else:
                    x[nbr] += alpha * xlast[n] * W[n][nbr].get(weight, 1) 
            x[n] += danglesum * dangling_weights[n] + (1.0 - alpha) * p[n] 

        # check convergence, l1 norm 
        err = sum([abs(x[n] - xlast[n]) for n in x]) 
        if err < N*tol: 
            return x 
    raise NetworkXError('pagerank: power iteration failed to converge in %d iterations.' % max_iter)

def modified_pagerank_graphframe(graphframe, alpha=0.85, personalization=None, 
                                max_iter=100, tol=1.0e-6, nstart=None, weight='weight', 
                                dangling=None):
    # Convert GraphFrame to NetworkX graph
    G = graphframe_to_networkx(graphframe)

    # Then apply the existing modified_pagerank function
    return modified_pagerank(G, alpha, personalization, max_iter, tol, nstart, weight, dangling)

def check_newData(file_path, last_process_datetime):
    while True:
        with open(file_path, 'r') as file:
            content = file.read().strip()
        if content != last_process_datetime:
            for i in range(10):
                print("New data: " + file_path)

            last_process_datetime = content
            return True, last_process_datetime
        else:
            for i in range(10):
                print ("[  " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "  ]   Waiting for new data to come in...")
                print ("[  " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "  ]   " + "Sleeping for 10 SECS...")
            time.sleep(10) 

newData_signal_filePath = "/home/ubermate/json/newData_signal.txt"    
permissionFilePath = "/home/ubermate/json/kmeanPermission.txt" 

last_process_datetime = ""
with open(newData_signal_filePath, 'r') as file:
        last_process_datetime = file.read().strip()

while True:
    newData_signal, last_process_datetime = check_newData(newData_signal_filePath, last_process_datetime)
    print("[  " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "  ]   last_process datetime: " + last_process_datetime)
    if newData_signal:
        with open(permissionFilePath, 'w') as file:
            file.write("kmean running now...")
        # Set the session
        spark = SparkSession.builder.appName("kmeanPagerank").enableHiveSupport().getOrCreate()

        # Set the current database
        spark.sql("USE default")

        # Query the database
        df = spark.sql("SELECT * FROM cleaned_table;")

        # "pulocationid", "dolocationid", 
        feature_columns = ["pickup_hour_of_week"]
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
        df = assembler.transform(df)

        # k-mean
        k = 7  
        kmeans = KMeans(k=k, featuresCol="features", predictionCol="cluster")
        model = kmeans.fit(df)
        results = model.transform(df)


        # Aggregating data
        cluster_summary = results.groupBy("cluster").agg(
            F.max("pickup_hour_of_week").alias("max_pickup_hour_of_week"),
            F.min("pickup_hour_of_week").alias("min_pickup_hour_of_week")
        )
        data = cluster_summary.collect()
        
        # Export to CSV
        output_file_of_time = f'timespanRange.csv'
        with open(output_file_of_time, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Cluster', 'Max Pickup Hour of Week', 'Min Pickup Hour of Week'])
            for row in data:
                writer.writerow([row['cluster'], row['max_pickup_hour_of_week'], row['min_pickup_hour_of_week']])
        print("Printed the timespanRange !!!!!!!!!!")
        # fixed totally 264 locationID in NYC
        vertex_ids = range(1, 264) 
        vertices_df = spark.createDataFrame([(id,) for id in vertex_ids], ["id"])

        #pagerank iteration
        cluster_ids = results.select("cluster").distinct().rdd.flatMap(lambda x: x).collect()

        for cluster_id in cluster_ids:
            # Filter the DataFrame for the current cluster
            cluster_df = results.filter(col("cluster") == cluster_id)
            edges_df = cluster_df.groupBy("pulocationid", "dolocationid") \
                    .count() \
                    .withColumnRenamed("count", "weight") \
                    .withColumnRenamed("pulocationid", "src") \
                    .withColumnRenamed("dolocationid", "dst")
            graph = GraphFrame(vertices_df, edges_df)

            # Apply modified PageRank
            pagerank = modified_pagerank_graphframe(graph)

            # Export to CSV
            output_file = f'pagerank_cluster_{cluster_id}.csv'
            with open(output_file, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Key', 'Value'])  # Writing header
                for key, value in pagerank.items():
                    writer.writerow([key, value])

            print(f"PageRank for cluster {cluster_id} written to {output_file}")
        spark.stop()
        with open(permissionFilePath, 'w') as file:
            file.write("kmean finsished")

