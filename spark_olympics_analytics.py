import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, lit, sum, when, row_number, upper, 
    count, desc
)
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.window import Window
def initialize_spark_session():
    return SparkSession.builder.appName("OlympicsAnalysis").getOrCreate()
def load_athlete_data(spark, file_path, year):
    return spark.read.csv(
        file_path, 
        header=True, 
        inferSchema=True
    ).withColumn("competition_year", lit(year))
def load_supporting_data(spark, medals_path, coaches_path):
    medals_df = spark.read.csv(medals_path, header=True, inferSchema=True)
    dfc = spark.read.csv(coaches_path, header=True, inferSchema=True)
    valid_years = ["2012", "2016", "2020"]
    return (
        dfc,
        medals_df.filter(col("year").isin(valid_years))
    )
def get_medal_points():
    return {
        2012: {"GOLD": 20, "SILVER": 15, "BRONZE": 10},
        2016: {"GOLD": 12, "SILVER": 8, "BRONZE": 6},
        2020: {"GOLD": 15, "SILVER": 12, "BRONZE": 7}
    }
def create_calpoints(points_system):
    def calculate_medal_points(med, year):
        year = int(year)
        med = med.upper() if med else ""
        return points_system.get(year, {}).get(med, 0)
    return calculate_medal_points
def combine_athlete_data(a12th, a16th, a20th):
    return a12th.union(a16th).union(a20th)
def find_best_athletes(athcom, m_f, calpoints):
    med = athcom.alias("athletes").join(
        m_f.alias("medals"),
        (col("athletes.id") == col("medals.id")) &
        (upper(col("athletes.sport")) == upper(col("medals.sport"))) &
        (upper(col("athletes.event")) == upper(col("medals.event"))) &
        (col("athletes.competition_year") == col("medals.year"))
    )
    perf_at = med.withColumn(
        "earned_points", 
        expr("calculate_medal_points(medal, medals.year)")
    )
    symm_perf = perf_at.groupBy(
        "name", 
        "athletes.sport"
    ).agg(
        sum("earned_points").alias("total_points"),
        sum(when(upper(col("medal")) == "GOLD", 1).otherwise(0)).alias("gold_medals"),
        sum(when(upper(col("medal")) == "SILVER", 1).otherwise(0)).alias("silver_medals"),
        sum(when(upper(col("medal")) == "BRONZE", 1).otherwise(0)).alias("bronze_medals")
    )
    win_r = Window.partitionBy(
        upper(col("sport"))
    ).orderBy(
        col("total_points").desc(),
        col("gold_medals").desc(),
        col("silver_medals").desc(),
        col("bronze_medals").desc(),
        upper(col("name"))
    )
    att = symm_perf.withColumn(
        "athlete_rank", 
        row_number().over(win_r)
    ).filter(
        col("athlete_rank") == 1
    ).select(
        "name", 
        "sport"
    ).orderBy(
        upper(col("sport"))
    )
    return [row.name.upper() for row in att.collect()]
def identify_c_t(athcom, dfc, m_f, calpoints):
    countar = ["CHINA", "INDIA", "USA"]
    ath_c = athcom.withColumn(
        "country", 
        upper(col("country"))
    ).filter(
        col("country").isin(countar)
    )
    perf_c = dfc.alias("coaches").join(
        ath_c.alias("athletes"),
        (upper(col("coaches.sport")) == upper(col("athletes.sport"))) &
        (col("coaches.id") == col("athletes.coach_id"))
    ).join(
        m_f.alias("medals"),
        (col("athletes.id") == col("medals.id")) &
        (upper(col("athletes.sport")) == upper(col("medals.sport"))) &
        (upper(col("athletes.event")) == upper(col("medals.event"))) &
        (col("athletes.competition_year") == col("medals.year"))
    ).distinct()
    pcoach = perf_c.withColumn(
        "earned_points", 
        expr("calculate_medal_points(medal, year)")
    )
    py = pcoach.groupBy(
        upper(col("coaches.name")).alias("coach_name"),
        col("athletes.country"),
        col("athletes.competition_year")
    ).agg(
        sum("earned_points").alias("yearly_points"),
        sum(when(upper(col("medal")) == "GOLD", 1).otherwise(0)).alias("yearly_gold"),
        sum(when(upper(col("medal")) == "SILVER", 1).otherwise(0)).alias("yearly_silver"),
        sum(when(upper(col("medal")) == "BRONZE", 1).otherwise(0)).alias("yearly_bronze")
    )
    pt = py.groupBy(
        col("coach_name"),
        col("country")
    ).agg(
        sum("yearly_points").alias("total_points"),
        sum("yearly_gold").alias("total_gold"),
        sum("yearly_silver").alias("total_silver"),
        sum("yearly_bronze").alias("total_bronze")
    )
    win_r = Window.partitionBy("country").orderBy(
        col("total_points").desc(),
        col("total_gold").desc(),
        col("total_silver").desc(),
        col("total_bronze").desc(),
        col("coach_name")
    )
    c_t = pt.withColumn(
        "coach_rank", 
        row_number().over(win_r)
    ).filter(
        col("coach_rank") <= 5
    )
    a = []
    for i in countar:
        tcc = c_t.filter(
            col("country") == i
        ).orderBy(
            "coach_rank"
        ).select(
            "coach_name"
        ).collect()
        a.extend([row.coach_name for row in tcc])
    return a
def write_results(output_path, best_athletes, c_t):
    o = f"({best_athletes},{c_t})".replace("'", '"')
    with open(output_path, 'w') as f:
        f.write(o)
def main():
    spark = initialize_spark_session()
    a12th = load_athlete_data(spark, sys.argv[1], 2012)
    a16th = load_athlete_data(spark, sys.argv[2], 2016)
    a20th = load_athlete_data(spark, sys.argv[3], 2020)
    dfc, m_f = load_supporting_data(spark, sys.argv[5], sys.argv[4])
    points_system = get_medal_points()
    calpoints = create_calpoints(points_system)
    spark.udf.register("calculate_medal_points", calpoints, IntegerType())
    athcom = combine_athlete_data(a12th, a16th, a20th)
    best_athletes = find_best_athletes(athcom, m_f, calpoints)
    c_t = identify_c_t(athcom, dfc, m_f, calpoints)
    write_results(sys.argv[6], best_athletes, c_t)
    spark.stop()
if __name__ == "__main__":
    main()
