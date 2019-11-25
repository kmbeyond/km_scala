
//Assign an ID for time-lapse
----------DATA--------
ID,START_TIME,END_TIME
100,10:00,12:00
100,10:15,12:30
100,12:15,12:45
100,13:00,14:00
200,10:15,10:30
-----------------------
Output:
+---+----------+--------+--------+                                              
|ID |START_TIME|END_TIME|GROUP_ID|
+---+----------+--------+--------+
|100|10:00     |12:00   |1       |
|100|10:15     |12:30   |1       |
|100|12:15     |12:45   |1       |
|100|13:00     |14:00   |2       |
|200|10:15     |10:30   |3       |
+---+----------+--------+--------+


val csvPath = "file:///home/kiran/km/km_big_data/data"

val fileOptions1 = Map(("header" -> "true"), ("delimiter" -> ","))
val dataDF = spark.read.options(fileOptions1).csv(csvPath+"/data_time_overlaps.csv")
var tblName="kmdb.km_time_overlaps"

//dataDF.write.insertInto(tblName)
dataDF.createOrReplaceTempView("km_time_overlaps");
spark.sql(s"CREATE TABLE ${tblName} AS SELECT * FROM km_time_overlaps")

spark.sql("""
 with data AS ( 
   SELECT ID,START_TIME,END_TIME FROM kmdb.km_time_overlaps
    ORDER BY ID,START_TIME,END_TIME
 )
 ,d AS (
  SELECT ID,START_TIME dt, 1 as INC FROM data
  UNION ALL
  SELECT ID,END_TIME dt, -1 as INC FROM data
 ),ds as (
      select ID, dt, sum(running_inc) over (partition by ID order by dt) as running_inc
      from (select ID, dt, sum(inc) as running_inc
            from d
            group by ID, dt
           ) d
 )
 ,g as (
      select ID, dt, sum(case when running_inc = 0 then 1 else 0 end) over (partition by ID order by dt desc) as grp
      from ds
 )
 ,id_dts AS (
  SELECT ID, min(dt) as start_time, max(dt) as end_time
   from g
    group by ID, grp ORDER BY ID, grp
 )
 ,id_dts_rank AS (
   SELECT ID,start_time,end_time, row_number() OVER (ORDER BY ID, start_time) AS group_id
    FROM id_dts
 )
 ,assign_grp AS (
  SELECT a.*, b.group_id, CASE WHEN a.START_TIME>=b.START_TIME AND a.END_TIME<=b.END_TIME THEN 1 ELSE 0 END AS valid
   FROM data a
    JOIN id_dts_rank b ON a.ID=b.ID
 )
 SELECT ID, START_TIME,END_TIME,GROUP_ID FROM assign_grp WHERE valid=1
   ORDER BY ID, START_TIME,END_TIME
""").show(false)

