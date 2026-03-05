# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, IntegerType, DateType, TimestampType, FloatType
from typing import List

# COMMAND ----------

import unidecode

# COMMAND ----------

def read_excel(path: str, start_cell: str = "A1", header: bool = True, infer_schema: bool = True,
               ignore_leading_whitespace: bool = True, treat_empty_values_as_nulls: bool = True,
               ignore_trailing_whitespace: bool = True, max_rows_in_memory: int = 2000) -> DataFrame:
    """
    Reads an Excel file into a PySpark DataFrame.

    Args:
    - path (str): The file path of the Excel file.
    - start_cell (str): The starting cell address of the data in the Excel sheet. Default is "A1".
    - header (bool): Whether the Excel file has a header row. Default is True.
    - infer_schema (bool): Whether to infer the schema from the Excel file. Default is True.
    - ignore_leading_whitespace (bool): Whether to ignore leading whitespace in column names. Default is True.
    - treat_empty_values_as_nulls (bool): Whether to treat empty cells as null values. Default is True.
    - ignore_trailing_whitespace (bool): Whether to ignore trailing whitespace in column names. Default is True.
    - max_rows_in_memory (int): Maximum number of rows to read into memory. Default is 1000.

    Returns:
    - PySpark DataFrame: A DataFrame containing the data from the Excel file.
    """
    df_excel = spark.read.format("com.crealytics.spark.excel") \
        .option("header", header) \
        .option("inferSchema", infer_schema) \
        .option("ignoreLeadingWhiteSpace", ignore_leading_whitespace) \
        .option("treatEmptyValuesAsNulls", treat_empty_values_as_nulls) \
        .option("ignoreTrailingWhiteSpace", ignore_trailing_whitespace) \
        .option("MaxRowsInMemory", max_rows_in_memory) \
        .option("dataAddress", start_cell) \
        .load(path)

    return df_excel

# COMMAND ----------

def find_columns_with_word(df: DataFrame, word: str) -> List:
    """
    Returns a list of column names in the PySpark DataFrame containing the specified word.

    Args:
    - df: PySpark DataFrame
    - word: Substring to search for in column names

    Returns:
    - List of column names containing the specified word
    """
    columns_with_word = [col_name for col_name in df.columns if word in col_name]
    return columns_with_word

# COMMAND ----------

def convert_to_date_format(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Converts columns in a PySpark DataFrame to date format.

    Args:
    - df: PySpark DataFrame
    - columns: List of column names to convert to date format

    Returns:
    - PySpark DataFrame with specified columns converted to date format
    """
    ### Set timeParcer para poder converter 97 para 1997
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    
    for column in columns:
        column_type = dict(df.dtypes)[column]
        try:
            if "timestamp" in column_type:
                df = df.withColumn(column, to_date(col(column), 'yyyy-MM-dd'))
            else:
                df = df.withColumn(column, when(
                                                length(col(column)) == 10,
                                                to_date(col(column), 'dd/MM/yyyy')
                                            ).otherwise(
                                                to_date(col(column), 'd/M/yy')
                                            )
                )
        except:
            print(f"Column '{column}' does not exist. Skipping...")
    
    ### Reverte setting do timeParcer para o defaut
    # spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

    return df

# COMMAND ----------

# Substitui caracteres das colunas especificadas
def replace_characters(df: DataFrame, columns: List[str], char_to_replace: str, replacement_char: str) -> DataFrame:
    """
    Replace characters in specified columns of a Spark DataFrame.

    Args:
    - df (DataFrame): The input Spark DataFrame.
    - columns (List[str]): List of column names where replacement should be performed.
    - char_to_replace (str): The character(s) to replace.
    - replacement_char (str): The character(s) to replace with.

    Returns:
    - DataFrame: The Spark DataFrame with characters replaced in specified columns.
    """
    for column in columns:
        try:
            df = df.withColumn(column, regexp_replace(col(column), char_to_replace, replacement_char))
        except:
            print(f"Column '{column}' does not exist. Skipping...")
    return df

# COMMAND ----------

# Ajuste dos valores percetuais
def convert_to_float(df: DataFrame, columns: List[str]):
    """
    Convert specified columns in a PySpark DataFrame to floating-point numbers.

    Args:
    - df (DataFrame): The input PySpark DataFrame.
    - columns (List[str]): List of column names to convert to float.

    Returns:
    - DataFrame: The PySpark DataFrame with specified columns converted to float.
    """
    for column in columns:
        try:
            df = df.withColumn(column, col(column).cast("float"))
        except:
            print(f"Column '{column}' does not exist. Skipping...")
    return df

# COMMAND ----------

def adjust_column_names(df: DataFrame) -> DataFrame:
    """
    Adjusts column names of a DataFrame by replacing spaces and special characters with underscores
    and converting the column names to upcase.
    
    Args:
        df (DataFrame): Input DataFrame
        
    Returns:
        DataFrame: DataFrame with adjusted column names
    """
    adjusted_df = df
    for column in df.columns:
        
        adjusted_column_name = column.replace('(', ' ').replace(')', ' ').replace('-', ' ').replace('.', ' ').replace('_', ' ').replace('/', ' ')
        adjusted_column_name = " ".join(adjusted_column_name.split())
        adjusted_column_name = adjusted_column_name.replace(' ', '_').upper()
        
        adjusted_df = adjusted_df.withColumnRenamed(column, adjusted_column_name)
    return adjusted_df

def adjust_list_names(lista: List[str]) -> List:
    """
    Adjusts column names of a DataFrame by replacing spaces and special characters with underscores
    and converting the column names to upcase.
    
    Args:
        df (DataFrame): Input DataFrame
        
    Returns:
        DataFrame: DataFrame with adjusted column names
    """

    adjusted_list = []
    for name in lista:
        
        adjusted_column_name = name.replace('(', ' ').replace(')', ' ').replace('.', ' ').replace('_', ' ').replace('/', ' ').replace('-', ' ')
        adjusted_column_name = " ".join(adjusted_column_name.split())
        adjusted_column_name = adjusted_column_name.replace(' ', '_').upper()
        adjusted_column_name = adjusted_list.append(adjusted_column_name)
    return adjusted_list

# COMMAND ----------

def compress_values(df: DataFrame, columns: list) -> DataFrame:
    """
    Trim and compress multiple columns in a PySpark DataFrame.
    
    Args:
    - df (DataFrame): Input PySpark DataFrame.
    - columns (list): List of column names to be compressed.
    
    Returns:
    - DataFrame: DataFrame with trimmed and compressed values in specified columns.
    """
    for column in columns:
        df = df.withColumn(column, regexp_replace(trim(df[column]), "\\s+", " "))
    return df

# COMMAND ----------

def compress_column_names(df: DataFrame) -> DataFrame:
    """
    Trim and compress the column names of a PySpark DataFrame.
    
    Args:
    - df (DataFrame): Input PySpark DataFrame.
    
    Returns:
    - DataFrame: DataFrame with trimmed and compressed column names.
    """
    # Create a dictionary to rename columns
    new_column_names = {col_name: ' '.join(col_name.split()) for col_name in df.columns}
    
    # Apply the new column names to the DataFrame
    df = df.select([col(old_name).alias(new_name) for old_name, new_name in new_column_names.items()])
    
    return df

# COMMAND ----------

# df = spark.createDataFrame([(1, 2)], ["  col 1  ", " col   2 "])
# df = compress_column_names(df)
# df.show()

# COMMAND ----------

def remove_acentos(df: DataFrame) -> DataFrame:
    """
    Remove acentos das colunas do DataFrane
    
    Args:
        df (DataFrame): Input DataFrame
        
    Returna:
        DataFrame: DataFrame com os nomes de colunas ajustados
    """
    adjusted_df = df
    for column in df.columns:
        
        adjusted_df = adjusted_df.withColumnRenamed(column, unidecode.unidecode(column))

    return adjusted_df

# COMMAND ----------

def deduplica_cols(df: DataFrame) -> DataFrame:
    """
    Remove colunas com mesmo nome do DataFrane
    Mantem a primeira encontrada

    Args:
        df (DataFrame): Input DataFrame
        
    Returna:
        DataFrame: DataFrame com colunas únicas
    """
    columns = df.columns
    renamed_columns = [f"{col}@{i}" for i, col in enumerate(columns)]
    
    # Rename columns to make them unique
    df_renamed = df.toDF(*renamed_columns)
    
    # Identify and keep the first occurrence of each column
    unique_columns = []
    seen = set()
    for i, col in enumerate(columns):
        if col not in seen:
            unique_columns.append(renamed_columns[i])
            seen.add(col)
    
    # Select only the unique columns
    df_unique = df_renamed.select(*unique_columns)
    
    # Rename columns back to original names
    original_names = [col.split('@')[0] for col in unique_columns]
    df_final = df_unique.toDF(*original_names)
    
    return df_final

# COMMAND ----------

def merge_dfs(source: DataFrame, new: DataFrame, join_cols: List[str]) -> DataFrame:
    """
    Um substituto ao MERGE do SAS.
    Realiza junção entre dois DataFrames do Spark e aplica coalesce em todas as colunas, 
    exceto nas colunas de junção.

    Parâmetros:
    source (DataFrame): DataFrame de origem.
    new (DataFrame): Novo DataFrame para a junção.
    join_cols (List[str]): Lista de nomes das colunas pelas quais os DataFrames serão unidos.

    Retorna:
    DataFrame: Resultado da junção e aplicação do coalesce.
    """
    # Generate the join condition dynamically
    join_condition = [source[col_name] == new[col_name] for col_name in join_cols]
    
    # Perform the left join
    joined_df = source.alias("a").join(new.alias("b"), on=join_condition, how='left')
    
    # Apply coalesce to all columns except the joining columns
    coalesce_exprs = [coalesce(col("b." + col_name), col("a." + col_name)).alias(col_name) for col_name in source.columns if col_name not in join_cols]
    
    # Select the required columns
    result_df = joined_df.select(*[col("a." + col_name) for col_name in join_cols], *coalesce_exprs)
    
    return result_df
