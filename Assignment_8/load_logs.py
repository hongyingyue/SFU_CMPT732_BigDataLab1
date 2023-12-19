from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import sys, os, gzip, re
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from datetime import datetime

def split_logs(line, pattern):
    match = pattern.match(line)
    if match:
        parts = match.groups()
        return parts


def main(input_dir, keyspace, table):
    session = cluster.connect(keyspace)

    logs_to_insert = []
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    for f in os.listdir(input_dir):
        with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                log = split_logs(line, line_re)
                # print(log)
                if log:
                    logs_to_insert.append(log)

    insert_log = session.prepare("INSERT INTO %s (id, host, datetime, path, bytes) VALUES (uuid(), ?, ?, ?, ?);" % table)
    batch_size_limit = 5  # split into smaller batches
    for i in range(0, len(logs_to_insert), batch_size_limit):
        batch = BatchStatement()
        for (host_name, dtime, path, request_bytes) in logs_to_insert[i:i + batch_size_limit]:
            dtime = datetime.strptime(dtime, "%d/%b/%Y:%H:%M:%S")
            rbytes = int(request_bytes)
            batch.add(insert_log, (host_name, dtime, path, rbytes))
        session.execute(batch)


if __name__ == '__main__':
    cluster = Cluster(['node1.local', 'node2.local'])
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    main(input_dir, keyspace, table)
