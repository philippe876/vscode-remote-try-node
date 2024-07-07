from datasets import load_dataset
import csv
import os
from tenacity import retry, stop_after_attempt, wait_exponential

# Define the output directory
output_dir = "/workspaces/vscode-remote-E/data/c42/"
filename = "filtered_c4_fr_datasetv2.csv"
os.makedirs(output_dir, exist_ok=True)

# Path to the output CSV file
csv_path = os.path.join(output_dir, filename)

# Retry settings: up to 5 attempts with exponential backoff
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
def load_and_process_data():
    # Load the French subset of the C4 dataset
    c4_fr = load_dataset("allenai/c4", "fr", streaming=True)

    # Open the CSV file for writing
    with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["split","url", "date", "size"])
        writer.writeheader()  # Write the header row

        # Iterate over the dataset and write each row to the CSV file
        for split in c4_fr.keys():
            for example in c4_fr[split]:
                filtered_row = {
                    "split": split,
                    "url": example["url"],
                    "date": example["timestamp"],
                    "size": len(example["text"])  # Assuming size is the length of the text
                }
                writer.writerow(filtered_row)

# Call the function to load and process data
try:
    load_and_process_data()
    print(f"CSV file saved to: {csv_path}")
except Exception as e:
    print(f"Failed to process data after several retries: {e}")
