from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setMaster("local").setAppName("Exam 1 - Part A")
sc = SparkContext(conf = conf)

studentsRdd = sc.textFile ("file:///studentsPR.csv")

students = studentsRdd.map(lambda line: line.split(','))

filtered = students.filter(lambda student: student[2] == "71381" and student[5] == "F")

filtered.collect()