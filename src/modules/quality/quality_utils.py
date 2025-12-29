from pyspark.sql import functions as F
from pyspark.sql.types import NumericType
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType
from datetime import timedelta

# ===================
# Profiling Functions
# ===================
def descriptive_profile(df: DataFrame) -> dict:
    """
    Data Profiling function to analyze a Spark dataFrame.
    Returns:
        A dictionary containing overall row count, missing counts per column,
        unique counts per column, and descriptive statistics for numerical columns.
    """

    # 1. Row and Column Count
    total_rows = df.count()
    total_cols = len(df.columns)
    
    # 2. Descriptive Statistics for numeric columns
    num_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, NumericType)]
    stats_num = df.select(num_cols).describe().toPandas() if num_cols else None
    
    # 3. Unique Counts per attribute
    unique_counts = {}
    for col in df.columns:
        count_unique = df.select(col).distinct().count()
        unique_counts[col] = count_unique
    
    # Pack the results into a dictionary
    return {
        "total_rows": total_rows,
        "total_cols": total_cols,
        "numeric_columns": num_cols,
        "descriptive_stats": stats_num,
        "unique_counts": unique_counts,
    }


def print_dataset_profile(results: dict) -> None:
    """
    Prints dataset profile with aligned statistics for multiple variables per row.
    """
    def create_section(title, data, is_stats=False):
        section = [f"{title}:", "-" * 40]

        if isinstance(data, str):
            section.append(data)
            return "\n".join(section)
        
        if is_stats:
            variables = sorted(results['numeric_columns'])
            if not variables:
                return "\n".join([*section, "No numeric columns"])
            
            # Prepare column headers
            col_width = 15
            header = "Statistic".ljust(12)
            header += "".join([f"{var:>{col_width}}" for var in variables])
            section.append(header)
            section.append("-" * (12 + col_width * len(variables)))
            
            # Process statistics in standard order
            stats_order = ['count', 'mean', 'stddev', 'min', 'max']
            for stat in stats_order:
                stat_row = data[data['summary'].str.lower() == stat]
                if stat_row.empty:
                    continue
                    
                values = []
                for var in variables:
                    val = stat_row[var].values[0]
                    try:
                        if stat == 'count':
                            formatted = f"{int(float(val))}"
                        else:
                            formatted = f"{float(val):.2f}"
                    except:
                        formatted = str(val)
                    values.append(formatted)
                
                stat_label = f"{stat.capitalize()}:".ljust(12)
                stat_line = stat_label + "".join([f"{v:>{col_width}}" for v in values])
                section.append(stat_line)
            
        else:
            for col in sorted(data.keys()):
                val = data[col]
                line = f"- {col}: {val}"
                section.append(line)
                
        return "\n".join(section)

    # Build output sections
    sections = [
        f"Total Rows: {results['total_rows']} \nTotal Columns: {results['total_cols']}",
        create_section("Unique Values Count", results['unique_counts']),
        create_section("Numeric Columns", {"columns": results['numeric_columns']} 
          if results['numeric_columns'] else "No numeric columns")
    ]
    
    if results['descriptive_stats'] is not None:
        sections.append(
            create_section("Descriptive Statistics", 
                          results['descriptive_stats'], 
                          is_stats=True)
        )
    
    return "\n\n".join(sections)

# =========================
# Quality Metrics Functions
# =========================
def compute_column_completeness(df):
    """Calculate the ratio of missing values for each column in the DataFrame."""
    total_rows = df.count()
    spark = df.sparkSession
    if total_rows == 0:
        return spark.createDataFrame([], schema=["column", "missing_ratio"])
    
    stats = []
    for column in df.columns:
        null_count = df.select(
            F.coalesce(F.sum(F.col(column).isNull().cast("int")), F.lit(0))
        ).first()[0]
        
        stats.append((column, float(null_count / total_rows)))

    # Create a DataFrame from the results
    return spark.createDataFrame(stats, ["column", "missing_ratio"])


def compute_relation_completeness(df):
    """Calculate the ratio of rows with no missing values in the DataFrame."""
    total_rows = df.count()
    if total_rows == 0 or len(df.columns) == 0:
        return 0.0
    
    # Create an expression that checks all columns are not null
    all_non_null_expr = F.expr(" AND ".join([f"({col} IS NOT NULL)" for col in df.columns]))
    # Calculate the average of the non-null indicator (gives the ratio)
    ratio_row = df.select(F.avg(all_non_null_expr.cast("float"))).first()[0]
    return ratio_row if ratio_row is not None else 0.0


def compute_attribute_timeliness(df: DataFrame, column: str, transactionTime_col: str ,update_rate: float = 12) -> DataFrame:
    """
    Compute attribute-level timeliness scores (Q_T_Ai).
        transactionTime_col: column with the transaction time of the data.
        update rate: assumed same update_rate for all values in a column. Default is 12 updates/day.
    """
    
    # Calculate age in days for each value: assume all values in a row have the same age
    df = df.withColumn("age_days", F.datediff(F.current_timestamp(), F.col(transactionTime_col)) + F.lit(1e-9)) # avoid division by zero
    
    # Calculate timeliness score per value: assume all have the same update rate
    df = df.withColumn("Q_T_v", F.lit(1) / (F.lit(1) + F.col("age_days") * F.lit(update_rate))
    )
    
    # Calculate average score for the attribute
    return df.agg(F.lit(column).alias("attribute"), F.avg("Q_T_v").alias("timeliness_score"))


def detect_TS_outliers(df, outlier_df, col_name, window_size=7, timestamp_col="datetime_iso"):
    """
    Detects outliers in time series data using rolling IQR.
    
    Args:
        df: Spark DataFrame with datetime column
        outlier_df: Accumulated DataFrame with outlier results
        col_name: Column to analyze for outliers
        window_size: Nº of rows of the rolling window for IQR calculations
        timestamp_col: Column with datetime values
    
    Returns:
        Updated outlier_df with new outlier columns
    """

    # Process a temporary DataFrame
    temp_df = df.orderBy(timestamp_col)
    
    # Calculate daily increases, imputing 0 for the first row
    window_spec = Window.orderBy(timestamp_col)
    temp_df = temp_df.withColumn(f"daily_increase_{col_name}", 
        F.col(col_name) - F.lag(col_name, 1).over(window_spec)).withColumn(f"daily_increase_{col_name}",
                                                                    F.when(F.col(f"daily_increase_{col_name}").isNull(), 0)
                                                                    .otherwise(F.col(f"daily_increase_{col_name}"))
                                                                    )
    
    # Rolling calculations
    rolling_window = Window.orderBy(timestamp_col).rowsBetween(-(window_size-1), 0)
    temp_df = temp_df.withColumn(
        f"rolling_Q1_{col_name}", 
        F.expr(f"percentile_approx(daily_increase_{col_name}, 0.25, 100)").over(rolling_window)
    ).withColumn(
        f"rolling_Q3_{col_name}", 
        F.expr(f"percentile_approx(daily_increase_{col_name}, 0.75, 100)").over(rolling_window)
    ).withColumn(
        f"rolling_IQR_{col_name}", 
        F.col(f"rolling_Q3_{col_name}") - F.col(f"rolling_Q1_{col_name}")
    ).withColumn(
        f"rolling_upper_{col_name}", 
        F.col(f"rolling_Q3_{col_name}") + F.lit(7.5) * F.col(f"rolling_IQR_{col_name}")
    )
    
    # Outlier flags
    temp_df = temp_df.withColumn(f"is_outlier_{col_name}", 
        F.col(f"daily_increase_{col_name}") > F.col(f"rolling_upper_{col_name}")
    )

    # Select PK column + outlier column and dialiy increase associated
    new_columns = [timestamp_col] + [c for c in temp_df.columns 
                                    if c.startswith(("daily_increase_", "is_outlier_")) and col_name in c]
    temp_outlier = temp_df.select(new_columns).dropDuplicates([timestamp_col])
    
    # Merge with outlier_df
    return outlier_df.join(temp_outlier, [timestamp_col], "left")


def interpolate_missing(df, date_col: str = "DATE") -> DataFrame:
    """Impute missing values using linear interpolation for numeric columns."""
    # Ensure the DataFrame is sorted by DATE to maintain chronological order
    df = df.orderBy(date_col)
    
    # Identify numeric columns (exclude DATE)
    numeric_cols = [col for col in df.columns
                    if col != date_col and isinstance(df.schema[col].dataType, NumericType)]
    
    for column in numeric_cols:
        # Define window ordered by DATE (spanning entire dataset)
        window_spec = Window.orderBy(date_col)

        # Previous non-null value (last non-null before current row)
        prev_val = F.last(F.col(column), ignorenulls=True).over(
            window_spec.rowsBetween(Window.unboundedPreceding, -1)
        )

        # Next non-null value (first non-null after current row)
        next_val = F.first(F.col(column), ignorenulls=True).over(
            window_spec.rowsBetween(1, Window.unboundedFollowing)
        )
        
        # Compute interpolated value
        interpolated = F.when(
            F.col(column).isNull(),
            F.when(
                prev_val.isNotNull() & next_val.isNotNull(),
                (prev_val + next_val) / 2  # Average if both exist
            ).when(
                prev_val.isNotNull(),
                prev_val  # Use previous if only it exists
            ).when(
                next_val.isNotNull(),
                next_val  # Use next if only it exists
            ).otherwise(F.lit(None))  # Leave as null if no values
        ).otherwise(F.col(column))  # Keep original value if not null

        # Update the column with interpolated values
        df = df.withColumn(column, interpolated)

    return df

def impute_with_mode(df):
    """Impute missing values with mode for specified categorical columns.
    
    Args:
        df: Input Spark DataFrame
        categorical_cols: List of categorical columns to impute.
                          If None, auto-detect string columns.
    
    Returns:
        DataFrame with imputed categorical columns
    """
    
    # Auto-detect string columns if not specified
    categorical_cols = [col for col, dtype in df.dtypes if dtype == 'string']
    print(categorical_cols)
    
    # 2. Efficient mode calculation using window functions
    modes = {}
    for col in categorical_cols:
        # Calculate mode with tie handling and null protection
        mode_df = (df
                   .groupBy(col)
                   .agg(F.count("*").alias("count"))
                   .withColumn("rank", F.dense_rank().over(Window.orderBy(F.desc("count"))))
                   .filter(F.col("rank") == 1)
                   .limit(1)
                  )
        
        # Handle empty results (all nulls in original column)
        mode_row = mode_df.first()
        modes[col] = mode_row[col] if mode_row else None

    # 3. Safely apply imputation with null checks
    for col in categorical_cols:
        mode_value = modes.get(col)
        
        if mode_value is not None:
            df = df.withColumn(
                col,
                F.when(F.col(col).isNull(), mode_value)
                 .otherwise(F.col(col))
            )
        else:
            # Handle all-null columns by filling with literal 'unknown'
            df = df.withColumn(
                col,
                F.when(F.col(col).isNull(), 'unknown')
                 .otherwise(F.col(col))
            )

    return df


def check_missing_dates(df: DataFrame, date_col: str = 'datetime_iso') -> dict:
    """
    Checks for missing dates in the dataset based on the 'datetime_iso' column.
    Returns a dictionary containing:
        - 'missing_dates': A list of dates that are missing.
        - 'all_dates': A list of all dates in the range.
        - 'is_complete': Boolean indicating if all dates are present in the range.
    """
    
    # Convert 'datetime_iso' to date type
    df = df.withColumn('date_only', F.to_date(F.col(date_col), 'yyyy-MM-dd'))
    
    # Get the minimum and maximum date from the column
    min_date = df.select(F.min('date_only')).first()[0]
    max_date = df.select(F.max('date_only')).first()[0]
    print("Range of dates: ", min_date,max_date)
    
    # Generate a sequence of dates from min_date to max_date using Python's timedelta
    date_range = []
    current_date = min_date
    while current_date <= max_date:
        date_range.append(current_date)
        current_date += timedelta(days=1)
    
    # Create a DataFrame with all dates in the range
    all_dates_df = df.sparkSession.createDataFrame([(d,) for d in date_range], ['date_only'])
    # Get the dates present in the dataset
    present_dates_df = df.select('date_only').distinct()
    # Find missing dates 
    missing_dates_df = all_dates_df.join(present_dates_df, on='date_only', how='left_anti')
    # Collect the missing dates
    missing_dates = [row['date_only'] for row in missing_dates_df.collect()]
    # Check if all dates are present
    is_complete = len(missing_dates) == 0
    
    return {
        'missing_dates': missing_dates,
        'all_dates': date_range,
        'is_complete': is_complete
    }


def interpolate_impute(df, columns_to_impute):
    """Impute missing values using temporal interpolation between adjacent measurements."""
    window_spec = Window.orderBy("datetime_iso")
    
    for col in columns_to_impute:
        # Get previous and next non-null values in time sequence
        df = (df
              .withColumn(f"prev_{col}", F.last(col, ignorenulls=True).over(window_spec))
              .withColumn(f"next_{col}", F.first(col, ignorenulls=True).over(
                  Window.orderBy(F.desc("datetime_iso")))))
        
        # Calculate interpolated value (average of adjacent measurements)
        df = df.withColumn(col, 
            F.when(F.col(col).isNull(),
                F.coalesce(
                    (F.col(f"prev_{col}") + F.col(f"next_{col}")) / 2,  # Interpolate
                    F.col(f"prev_{col}"),  # Forward fill if no next
                    F.col(f"next_{col}"),  # Backward fill if no prev
                    F.lit(0)              # Fallback
                )
            ).otherwise(F.col(col))
        ).drop(f"prev_{col}", f"next_{col}")
    
    return df

def impute_numerical(df, numerical_cols: list = None, strategy: str = 'median', default_fill: float = 0.0):
    """
    Impute missing values in numerical columns using specified strategy.
    
    Args:
        df: Input Spark DataFrame
        numerical_cols: List of numerical columns to impute (None = auto-detect)
        strategy: Imputation strategy - 'median' (default) or 'mean'
        default_fill: Default value for all-null columns
    
    Returns:
        Imputed DataFrame
    """
    # 1. Auto-detect numerical columns if not specified
    if numerical_cols is None:
        numerical_cols = [col for col, dtype in df.dtypes if dtype in ['int', 'bigint', 'double', 'float']]
    
    # 2. Calculate imputation values
    impute_values = {}
    
    for col in numerical_cols:
        if strategy == 'median':
            # Efficient approximate median calculation
            quantiles = df.approxQuantile(col, [0.5], 0.01)
            imp_value = quantiles[0] if quantiles else default_fill
        elif strategy == 'mean':
            # Protected mean calculation
            mean_row = df.agg(F.mean(F.col(col)).alias('mean')).first()
            imp_value = mean_row['mean'] if mean_row and mean_row['mean'] is not None else default_fill
        else:
            raise ValueError(f"Invalid strategy: {strategy}. Choose 'median' or 'mean'")
        
        impute_values[col] = imp_value
    
    # 3. Apply imputation using coalesce
    for col in numerical_cols:
        df = df.withColumn(
            col,
            F.coalesce(
                F.col(col),
                F.lit(impute_values[col])
        ))
    return df