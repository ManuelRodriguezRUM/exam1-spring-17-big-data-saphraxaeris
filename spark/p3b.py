from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setMaster("local").setAppName("Exam 1 - Part B")
sc = SparkContext(conf = conf)

schoolsRdd = sc.textFile("file:///escuelasPR.csv")

schools = schoolsRdd.map(lambda line: line.split(','))

filtered = schools.filter(lambda student: schools[2] == "Ponce" or schools[2] == "Cabo Rojo" or schools[2] == "Dorado")

filtered.collect()