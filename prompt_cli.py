from runtime_analytics.loader import load_logs
from runtime_analytics.analyzer import sns_duration_by_riskdate

def run_cli():
    path = input("Enter path to log file: ").strip()
    df = load_logs(path)

    while True:
        query = input("\nAsk a question (or 'exit'): ").lower().strip()
        if query == "exit":
            break

        if "sns" in query and "duration" in query and "riskdate" in query:
            result = sns_duration_by_riskdate(df)
            print(result.to_string(index=False))

        elif "total jobs by type" in query:
            print(df["type"].value_counts())

        else:
            print("Unrecognized query. Try another.")

if __name__ == "__main__":
    run_cli()
