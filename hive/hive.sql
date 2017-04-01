CREATE DATABASE exam1db;

USE exam1db;

CREATE TABLE schools (region String, district String, city String, school_id int, name String, level String, college_board_id int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ‘,’
LINES TERMINATED BY ‘\n’;

LOAD DATA LOCAL INPATH 'escuelasPR.csv'
OVERWRITE INTO TABLE schools;

CREATE TABLE students (region String, district String, school_id int, name String, level varchar(3), sex varchar(1), student_id int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ‘,’
LINES TERMINATED BY ‘\n’;

LOAD DATA LOCAL INPATH 'studentsPR.csv'
OVERWRITE INTO TABLE students;

SELECt region, city, COUNT(*)
FROM schools
GROUP BY region, city;

SELECT sch.name, COUNT(*)
FROM students stu
JOIN schools sch
ON (stu.school_id = sch.school_id)
GROUP BY stu.school_id;

SELECT stu.name, stu.student_id,
FROM students stu
JOIN schools sch
ON (stu.school_id = sch.school_id)
WHERE sch.city = "Ponce" OR sch.city = "Cabo Rojo"

SELECT stu.region, sch.city, COUNT(*)
FROM students stu
JOIN schools sch
ON (stu.school_id = sch.school_id)
GROUP BY stu.region, sch.city;
