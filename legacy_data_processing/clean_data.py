import pandas as pd
def clean_data():
    """
        Cleans the data in the legacy csv files.

        This performs the following cleaning operations:
        - Drops any rows with missing values
        - Converts the id columns to integers

        After cleaning the data, the function saves the 
        cleaned data back to the respective CSV files.

        Note: This function assumes that the CSV files 
        are located in the 'data' directory.

        Example usage:
        clean_data()
    """
    for file in os.listdir("data"):
        if file.endswith(".csv"):
            df = pd.read_csv(f"data/{file}")
            df.dropna(inplace=True)
            # convert id columns to integers
            for col in df.columns:
                if "id" in col:
                    df[col] = df[col].astype(int)
            df.to_csv(f"data/{file}", 
                        index=False,
                        sep=",", 
                        header=None)
    print("Data cleaned successfully!")

if __name__ == "__main__":
    clean_data()
