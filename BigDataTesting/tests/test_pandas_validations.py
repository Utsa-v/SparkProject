import pandas as pd

# Load the data
source_df = pd.read_csv(r'C:\Users\Utsav Sharma\PycharmProjects\BigDataTesting\data\source_sales.csv')
target_df = pd.read_csv(r'C:\Users\Utsav Sharma\PycharmProjects\BigDataTesting\data\target_sales.csv')

def test_row_count_match():
    assert len(source_df) == len(target_df), \
        f"Row count mismatch: {len(source_df)} != {len(target_df)}"

def test_null_order_ids():
    nulls = source_df[source_df['order_id'].isnull()]
    assert nulls.empty, f"Found null order_id rows:\n{nulls}"

def test_transformation_logic():
    # Calculate expected total
    source_df['expected_total'] = source_df['quantity'] * source_df['price']

    # Merge with target to compare
    merged = pd.merge(source_df, target_df, on=['order_id', 'product', 'order_date'])

    mismatches = merged[merged['expected_total'] != merged['total_amount']]

    assert mismatches.empty, f"Transformation mismatches found:\n{mismatches[['order_id', 'expected_total', 'total_amount']]}"

def test_duplicates_in_source():
    duplicates = source_df[source_df.duplicated(['order_id'])]
    assert duplicates.empty, f"Duplicate order_ids found:\n{duplicates}"
