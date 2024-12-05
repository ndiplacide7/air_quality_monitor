from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, col, when, sum
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType


class AirQualityMapReduce:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("AirQualityAnalysis") \
            .getOrCreate()

    def analyze_air_quality_data(self, hdfs_path):
        """
        Perform comprehensive analysis of air quality data stored in HDFS

        :param hdfs_path: Path to the air quality readings data in HDFS
        :return: Dictionary containing various aggregated analysis results
        """
        # Read the air quality data from HDFS
        df = self.spark.read.parquet(hdfs_path)

        # Overall air quality statistics
        overall_stats = df.agg(
            avg('aqi').alias('avg_aqi'),
            max('aqi').alias('max_aqi'),
            min('aqi').alias('min_aqi'),
            avg('pm25').alias('avg_pm25'),
            avg('pm10').alias('avg_pm10')
        ).collect()[0]

        # Analyze pollutant levels by station
        station_pollutant_analysis = df.groupBy('station_id').agg(
            avg('pm25').alias('avg_pm25'),
            avg('pm10').alias('avg_pm10'),
            avg('ozone').alias('avg_ozone'),
            avg('nitrogen_dioxide').alias('avg_nitrogen_dioxide'),
            avg('carbon_monoxide').alias('avg_carbon_monoxide'),
            avg('sulfur_dioxide').alias('avg_sulfur_dioxide'),
            avg('aqi').alias('avg_aqi')
        )

        # Identify stations with highest pollution levels
        top_polluted_stations = station_pollutant_analysis.orderBy(
            col('avg_aqi').desc()
        ).limit(10).collect()

        # Time-based analysis (assuming timestamp column exists)
        time_based_analysis = df.groupBy(
            self.spark.sql("date_trunc('day', timestamp)").alias('day')
        ).agg(
            avg('aqi').alias('daily_avg_aqi'),
            max('aqi').alias('daily_max_aqi')
        ).orderBy('day')

        # AQI Category Distribution
        aqi_categories = df.withColumn('aqi_category',
                                       when((col('aqi') >= 0) & (col('aqi') <= 50), 'Good')
                                       .when((col('aqi') > 50) & (col('aqi') <= 100), 'Moderate')
                                       .when((col('aqi') > 100) & (col('aqi') <= 150), 'Unhealthy for Sensitive Groups')
                                       .when((col('aqi') > 150) & (col('aqi') <= 200), 'Unhealthy')
                                       .when((col('aqi') > 200) & (col('aqi') <= 300), 'Very Unhealthy')
                                       .otherwise('Hazardous')
                                       )

        aqi_category_distribution = aqi_categories.groupBy('aqi_category').count().orderBy('count', ascending=False)

        # Trend Analysis using Window Functions
        window_spec = Window.partitionBy('station_id').orderBy('timestamp')

        # Identify significant pollution spikes
        pollution_trend = df.withColumn(
            'aqi_change',
            col('aqi') - when(
                col('lag("aqi", 1).over(window_spec)').isNotNull(),
                col('lag("aqi", 1).over(window_spec)')
            ).otherwise(col('aqi'))
        )

        significant_spikes = pollution_trend.filter(
            col('aqi_change') > 50  # Significant increase in AQI
        ).select('station_id', 'timestamp', 'aqi', 'aqi_change')

        # Return analysis results
        return {
            'overall_stats': overall_stats,
            'station_pollutant_analysis': station_pollutant_analysis.collect(),
            'top_polluted_stations': top_polluted_stations,
            'time_based_analysis': time_based_analysis.collect(),
            'aqi_category_distribution': aqi_category_distribution.collect(),
            'significant_pollution_spikes': significant_spikes.collect()
        }

    def export_analysis_results(self, analysis_results, output_path):
        """
        Export analysis results to various formats

        :param analysis_results: Dictionary of analysis results
        :param output_path: HDFS path to export results
        """
        # Convert analysis results to Spark DataFrames for export
        for key, value in analysis_results.items():
            if isinstance(value, list):
                # Create DataFrame from list of results
                if value:  # Check if list is not empty
                    df = self.spark.createDataFrame(value)
                    # Export as Parquet for efficient storage
                    df.write.mode('overwrite').parquet(f"{output_path}/{key}")

    def close(self):
        """
        Close the Spark session
        """
        self.spark.stop()


# Example usage
def main():
    air_quality_analyzer = AirQualityMapReduce()
    try:
        # Analyze air quality data from HDFS
        analysis_results = air_quality_analyzer.analyze_air_quality_data('/path/to/air/quality/data')

        # Export analysis results
        air_quality_analyzer.export_analysis_results(
            analysis_results,
            '/path/to/output/analysis'
        )

        # Print some key insights
        print("Overall AQI Statistics:")
        print(analysis_results['overall_stats'])

        print("\nTop 5 Most Polluted Stations:")
        for station in analysis_results['top_polluted_stations'][:5]:
            print(station)

    except Exception as e:
        print(f"Error in air quality analysis: {e}")

    finally:
        air_quality_analyzer.close()


if __name__ == '__main__':
    main()