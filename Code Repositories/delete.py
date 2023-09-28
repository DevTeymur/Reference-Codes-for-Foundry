# This code simply deletes all the data inside the output dataset adding a dummy row from the input dataset

from transforms.api import transform, Input, Output

@transform(
    input_df=Input('INPUT_DATASET_RID'),
    output_df=Output('OUTPUT_DATASET_RID')
)
def incremental_filter(input_df, output_df):
    new_df = input_df.dataframe()
    output_df.write_dataframe(new_df)
