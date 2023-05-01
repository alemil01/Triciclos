from pyspark import SparkContext
import sys
import itertools

def get_edges_rep(line):
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1 != n2:
        return [(n1, n2), (n2, n1)]
    else:
        return [(n1, n2)] #n1 == n2

def adjacents(sc, filename):
    edges = sc.textFile(filename).\
        map(lambda x: tuple(x.split(','))).\
        groupByKey().\
        mapValues(set).\
        mapValues(sorted).\
        map(lambda x: (x[0], list(filter(lambda y: y > x[0], x[1]))))
    
    adj = edges.collect()
    for node in adj:
        print(node[0], list(node[1]))
    return edges

def convListTriciclo(listaAsociada):
    triciclo = []
    for nodo in listaAsociada[1]:
        if nodo != 'exists':
            triciclo.append((nodo[1],)+listaAsociada[0])
    return triciclo

def triciclos(sc, filename):
    adj = adjacents(sc, filename)

    exists = adj.flatMapValues(lambda x: x).\
        map(lambda x: (x, 'exists'))

    pending = adj.flatMapValues(lambda x: itertools.combinations(x,2)).\
                                map(lambda x: (x[1], ('pending', x[0])))

    listaAsociada = exists.union(pending)

    salida = listaAsociada.groupByKey().\
                            mapValues(list).\
                            filter(lambda x: len(x[1])>1).\
                            flatMap(convListTriciclo)
    salida=salida.collect()
    print(salida)
    return salida


        
def main(filename):
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        triciclos(sc, filename)
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Uso python3 {0} <file>".format(sys.argv[0]))
    else:
        main(sys.argv[1])
