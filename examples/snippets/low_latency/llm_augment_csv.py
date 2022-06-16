import pandas as pd
import tamr_toolbox as tbox


# Initialize Tamr Client -- note port must be 9170 for low-latency match service
llm_client = tbox.utils.client.create(username="user", password="pw", host="localhost", port=9170)

# Get source data and rename columns to match project unified dataset as needed
filename = "/Data/source_data/incoming_employees.csv"
employee_data = pd.read_csv(filename)
employee_data.rename(columns={"last_name": "family_name"})

# Get LLM results and add them into the data
lookup_results = tbox.low_latency.llm.llm_query(
    llm_client,
    project_name="minimal_mastering",
    records=employee_data.to_dict("records"),
    type="clusters",
    max_num_matches=1,
)
employee_data["tamr_id"] = employee_data.index.map(
    lambda k: lookup_results[k][0]["matchedRecordId"] if lookup_results[k] else None
)
