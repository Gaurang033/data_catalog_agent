import pandas as pd

data = {
    "invoice_id": ["INV1001", "INV1002", "INV1003"],
    "store_name": ["LiquorMart NYC", "Cheers Chicago", "The Bottle Stop"],
    "product": ["Jack Daniels", "Absolut Vodka", "Bacardi Rum"],
    "category": ["Whiskey", "Vodka", "Rum"],
    "quantity": [3, 2, 5],
    "unit_price": [30, 25, 20],
    "total_amount": [90, 50, 100],
    "date": ["2024-05-01", "2024-05-02", "2024-05-03"],
    "customer_age": [35, 28, 40],
    "location": ["New York, NY", "Chicago, IL", "Miami, FL"],
}

df = pd.DataFrame(data)
df.to_excel("data/raw_pos.xlsx", index=False)
df.to_parquet("data/raw_pos.parquet")
