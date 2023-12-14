from pyspark.sql import SparkSession, functions, types, Row
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def main(inputs, output, s_node, d_node):
    raw_schema = types.StructType([
        types.StructField('out', types.StringType()),
        types.StructField('in', types.StringType()),
    ])
    graph_edges = spark.read.csv(inputs, schema=raw_schema, sep=':').cache()
    # graph_edges.show()

    ke_schema = types.StructType([
        types.StructField('node', types.StringType()),
        types.StructField('source', types.StringType()),
        types.StructField('distance', types.LongType()),
    ])
    known_edges = spark.createDataFrame([(s_node, '-', 0)], schema=ke_schema)

    used = set()
    for i in range(6):
        if not known_edges.filter(functions.col('distance') == i).count():
            break
        # via_nodes: List of Rows
        via_nodes_rows = known_edges.filter(functions.col('distance') == i).select('node').collect()
        via_nodes = [node[0] for node in via_nodes_rows]
        used = used.union(set(via_nodes))
        # print(via_nodes)
        # print(used)

        if d_node in via_nodes:
            break

        for sn in via_nodes: # sn: string
            # next_nodes: List of nodes
            next_nodes = graph_edges.filter(functions.col('out') == sn).select('in').collect()[0][0].strip().split()
            if not next_nodes:
                continue
            df_via = spark.createDataFrame([], schema=ke_schema)
            for nn in next_nodes:
                if nn in used:  # if a node appears in formed shorted path, skip the following operation
                    continue
                new_row = spark.createDataFrame([(nn, sn, i+1)], schema=ke_schema)
                df_via = df_via.union(new_row)
            known_edges = known_edges.unionAll(df_via).cache()

        known_edges.write.csv(output + '/iter-' + str(i), mode='overwrite')

    # known_edges.show()
    res = [d_node]
    vn = d_node
    while vn != s_node:
        if not known_edges.filter(functions.col('node') == vn).count():
            print('--- There is not a path from the start point to the destination.')
            print('---- No output files will be saved.')
            break
        vn = known_edges.filter(functions.col('node') == vn).select('source').collect()[0][0]
        res.append(vn)
    # print(res)
    finalpath = spark.createDataFrame([(val,) for val in res[::-1]]).coalesce(1)
    finalpath.write.csv(output + '/path', mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('shortest path df').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    inputs = sys.argv[1]
    output = sys.argv[2]
    s_node = sys.argv[3]
    d_node = sys.argv[4]
    main(inputs, output, s_node, d_node)
