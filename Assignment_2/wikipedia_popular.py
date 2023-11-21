from pyspark import SparkConf, SparkContext
import sys

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+


def split_and_convert(line):
    parts = line.split()
    view_counts = int(parts[3])
    tuple_record = (parts[0], parts[1], parts[2], view_counts, parts[4])
    yield tuple_record

def rm_noneed(record):
    slot, lang, title, cnt, byt = record
    return lang == 'en' and title != 'Main_Page' and not title.startswith('Special:')

def get_kv(record):
    slot, lang, title, cnt, byt = record
    return (slot, (cnt, title))

def get_key(kv):
    return kv[0]

def get_max(x, y):
    if x[0] > y[0]:
        return x
    else:
        return y

def tab_separated(kv):
    content = kv[1]
    return "%s\t(%s, %s)" % (kv[0], content[0], content[1])

text = sc.textFile(inputs)
records = text.flatMap(split_and_convert)
records_need = records.filter(rm_noneed)
views = records_need.map(get_kv)
viewmax = views.reduceByKey(get_max)
max_count = viewmax.sortBy(get_key)

max_count.map(tab_separated).saveAsTextFile(output)