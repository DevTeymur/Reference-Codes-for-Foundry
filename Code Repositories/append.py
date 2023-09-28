# This script simply appends rows from the input dataset to the output dataset

from transforms.api import transform, incremental, Input, Output, configure
from pyspark.sql import types as T

schema = T.StructType([
    T.StructField('YOUR_TIMESTAMP_COLUMN_NAME', T.TimestampType()),
    T.StructField('YOUR_STRING_COLUMN_NAME', T.StringType()),
    T.StructField('YOUR_INTEGER_COLUMN_NAME', T.LongType()),
    T.StructField('YOUR_FLOAT_COLUMN_NAME', T.DoubleType()),
])

FILE_COUNT_LIMIT = 100


@configure(profile=['KUBERNETES_NO_EXECUTORS'])
@incremental(require_incremental=True, snapshot_inputs=["input_df"])
@transform(
    input_df=Input('INPUT_DATASET_RID'),
    output_df=Output('OUTPUT_DATASET_RID')
)
def incremental_filter(input_df, output_df):
    new_df = input_df.dataframe()

    files = list(output_df.filesystem(mode='previous').ls())
    if (len(files) > FILE_COUNT_LIMIT):
        new_df = new_df.unionByName(output_df.dataframe('previous', schema))
        mode = 'replace'
    else:
        mode = 'modify'

    new_df = new_df.select(
        "YOUR COLUMN NAMES HERE AS A TUPLE"
    )

    output_df.set_mode(mode)
    output_df.write_dataframe(new_df.coalesce(1))
    #https://stackoverflow.com/questions/75825270/how-do-i-ensure-my-dataset-does-not-accumulate-a-lot-of-small-files-when-running/75825271#75825271
