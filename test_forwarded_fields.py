import pandas as pd
import sys

# Read the CSV
print("Loading CSV...")
df = pd.read_csv("data/2025-11-12.csv", sep="\t", encoding='utf-8')

print(f"\nTotal rows: {len(df)}")
print(f"\nColumns: {df.columns.tolist()}")

# Check forwarded emails
forwarded = df[df["Email Send Type"] == "Forwarded"]
print(f"\nForwarded emails: {len(forwarded)}")

# Check Email Send Date
print("\n--- Email Send Date Analysis ---")
print(f"Blank Email Send Date in forwarded: {forwarded['Email Send Date'].isna().sum() + (forwarded['Email Send Date'] == '').sum()}")
print(f"Sample forwarded Email Send Dates:")
print(forwarded[["Email Name", "Email Send Date"]].head(10))

# Check Email Address
print("\n--- Email Address Analysis ---")
blank_addresses = forwarded['Email Address'].isna().sum() + (forwarded['Email Address'] == '').sum()
print(f"Blank Email Address in forwarded: {blank_addresses}/{len(forwarded)}")
print(f"Populated Email Address in forwarded: {len(forwarded) - blank_addresses}/{len(forwarded)}")
print(f"Sample forwarded Email Addresses:")
print(forwarded[["Email Name", "Email Address"]].head(10))

# Check Last Activated by User
print("\n--- Last Activated by User Analysis ---")
print(f"Blank Last Activated by User in forwarded: {forwarded['Last Activated by User'].isna().sum() + (forwarded['Last Activated by User'] == '').sum()}")
print(f"Sample forwarded Last Activated by User:")
print(forwarded[["Email Name", "Last Activated by User"]].head(10))

# Check regular sends for comparison
regular = df[df["Email Send Type"] != "Forwarded"]
print(f"\n--- Regular Sends for Comparison ---")
print(f"Blank Email Send Date in regular: {regular['Email Send Date'].isna().sum() + (regular['Email Send Date'] == '').sum()}")
print(f"Blank Last Activated by User in regular: {regular['Last Activated by User'].isna().sum() + (regular['Last Activated by User'] == '').sum()}")
