from pyspark import SparkConf, SparkContext
import sys
import re
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def mapping(line):
    kvs = line.split(':')
    wordsep = re.compile(r'[%s\s]+')
    vs = wordsep.split(kvs[1])
    if len(vs)>1:
        return (int(kvs[0]),vs[1:])
    else:
        return (int(kvs[0]),None)

def sep(line):
    l2 = []
    if line[1] != None:
        for i in line[1]:
            if i != '':
                l2.append((line[0],int(i)))
        return l2
    else:
        return [(line[0],line[1])]

def shortest(a,b):
    if a[1] > b[1]:
        return b
    else:
        return a

def main(inputs, output, source, target):

    data = sc.textFile(inputs + '/links-simple-sorted.txt')
    data1 = data.map(mapping)
    source = int(source)
    target = int(target)

    data2 = data1.flatMap(sep).filter(lambda x: x[1] != None).cache()

    l1 = [(source, (-1,0))]
    knownpaths = sc.parallelize(l1).cache()
    tovisit = [source]

    for i in range(6):

        neighbours = data2.filter(lambda x: x[0] in tovisit).cache()
        newpaths = knownpaths.join(neighbours)
        newpaths = newpaths.map(lambda x: (x[1][1],(x[0],x[1][0][1]+1)))
        knownpaths = sc.union([knownpaths,newpaths])

        paths = knownpaths.reduceByKey(shortest).cache()

        paths.saveAsTextFile(output + '/iter-' + str(i))

        tovisit = neighbours.map(lambda x: x[1]).collect() # Here only the neighbouring nodes are being collected in a list. So the list length is not that big.

        if target in tovisit:
            break

    list2 = paths.map(lambda x: x[0]).collect() # Here only the 'to' nodes will be stored. So again, the list is not lenghty.
    if target in list2: 
        finalpath = []
        node = target
        finalpath.insert(0,node)
        while node != source:
            node = paths.filter(lambda x: x[0] == node).collect()[0][1][0] #here also the length of the list is not big as the process happens after filterring.
            finalpath.insert(0,node)

    else:
        finalpath = ["Target not found"]

    sc.parallelize(finalpath).coalesce(1).saveAsTextFile(output + '/path')



if __name__ == '__main__':
    conf = SparkConf().setAppName('shortest path')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    source = sys.argv[3]
    target = sys.argv[4]
    main(inputs, output, source, target)
