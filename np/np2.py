import logging
import pandas as pd
 
logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s - %(levelname)s : %(message)s "
)
 
logger = logging.getLogger("np2.py")
 
class UsageProcessor:
 
 
    def __init__(self,file_path = None,dataframe = None):
        if file_path is None and dataframe is None:
            raise ValueError("Provide any one - filepath or dataframe")
 
        if file_path is not None and dataframe is not None:
            raise ValueError("Don't provide both,One is Enough")
 
        self.fp = file_path
 
        if dataframe is not None:
            self.df = dataframe.copy()
 
        self.grid_hour_df = None
        self.daily_summary = None
        self.grid_summary = None
 
        logging.info("Successfully Created the UsageProcessor Class")
 
       
       
 
    def load_data(self):
 
        if self.fp is not None:
            self.df = pd.read_csv(self.fp)
 
        if self.df is None:
            raise ValueError("Dataframe is Empty")
 
        logging.info("Successfully Loaded the data")
 
        return self.df
       
 
    def clean_data(self):
 
        columns_mapping = {
                "datetime" : "timestamp",
                "CellID" : "grid_id",
                "countrycode" :"country_code",
                "smsin" :"sms_in",
                "smsout" : "sms_out",
                "callin" : "call_in",
                "callout" : "call_out",
                "internet" : "internet"
            }
 
        required_columns = {
                "timestamp",
                "grid_id",
                "country_code",
                "sms_in",
                "sms_out",
                "call_in",
                "call_out",
                "internet"
            }
 
        activity_columns = [
            "grid_id",
            "country_code",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet"
        ]
 
        if self.df is None:
            raise ValueError("Dataframe is Empty")
 
        self.df = self.df.rename(columns = columns_mapping)
       
       
 
        if not required_columns.issubset(set(self.df.columns)):
            logging.error("Missing columns found")
            raise ValueError("Missing columns : "+", ".join(sorted(required_columns)))
 
        self.df["timestamp"] = pd.to_datetime(
                               self.df["timestamp"],
                               errors = "coerce"
                               )
 
        for col in activity_columns:
            self.df[col] = pd.to_numeric(self.df[col],errors = "coerce")
 
        # negative rows
 
        negative_rows = (self.df[activity_columns] < 0).any(axis=1)
        negative_rows_count = negative_rows.sum()
 
        if negative_rows_count > 0:
            logging.error("Dataframe contains Negative Values")
            raise ValueError(f"Dataset contains {negative_rows_count} Negative_rows")
 
 
        #Null values
 
        null_rows = (self.df["grid_id"].isna() |
                     self.df["timestamp"].isna())
 
        null_rows_count = null_rows.sum()
 
        if null_rows_count > 0 :
            logging.info(f"Total Null rows Present : {null_rows_count}")
            self.df = self.df[~null_rows].copy()
 
 
        logging.info("Successfully Cleaned the data")
        return self.df
 
 
    def derive_time_features(self):
 
        self.df["hour"] = self.df["timestamp"].dt.hour
 
        self.df["day_of_week"] = self.df["timestamp"].dt.day_name()
        self.df["date"] = self.df["timestamp"].dt.date
 
        logging.info("Derived time Features from the dataframe")
 
        return self.df
 
    def aggregate_to_grid_time(self):
 
        required_columns = ["date","hour","grid_id"]
 
        if not set(required_columns).issubset(self.df.columns) :
            logging.error("Required Columns is Missing")
            raise ValueError("Missing columns" + ", ".join(sorted(required_columns)))
 
        self.grid_hour_df = self.df.groupby(required_columns,as_index =False).agg(
            sms_in = ("sms_in","sum"),
            sms_out = ("sms_out","sum"),
            call_in = ("call_in","sum"),
            call_out = ("call_out","sum"),
            internet = ("internet","sum")
        )
 
        logging.info("Successfully Aggregated to Grid-Time features")
        return self.grid_hour_df
         
       
 
    def derive_activity_features(self):
        if self.grid_hour_df is None:
            raise ValueError("Run aggregate_to_grid_time() Before running this function")
 
        self.grid_hour_df["total_sms"] = self.grid_hour_df["sms_in"] + self.grid_hour_df["sms_out"]
        self.grid_hour_df["total_calls"] = self.grid_hour_df["call_in"] + self.grid_hour_df["call_out"]
        self.grid_hour_df["total_activity"] = self.grid_hour_df["total_sms"] + self.grid_hour_df["total_calls"] + self.grid_hour_df["internet"]
 
        logging.info("derived Activity Features successfully")
 
        return self.grid_hour_df
                       
 
    def compute_kpis(self):
        if self.grid_hour_df is None:
            raise ValueError("Run derive_activity_feature() before running this function")
 
 
        self.daily_summary = self.grid_hour_df.groupby("date",as_index = False).agg(
            total_sms = ("total_sms","sum"),
            total_calls = ("total_calls","sum"),
            total_activity = ("total_activity","sum"),
            total_internet = ("internet","sum"),
            unique_grids = ("grid_id","nunique")
        )
 
        self.grid_summary = self.grid_hour_df.groupby("grid_id",as_index = False).agg(
                    total_sms = ("total_sms","sum"),
                    total_calls = ("total_calls","sum"),
                    total_activity = ("total_activity","sum"),
                    total_internet = ("internet","sum"),
                    active_hours = ("hour","nunique")
                )
 
        logging.info("Successfully Computed KPIs")
 
        return {
            "daily_summary" : self.daily_summary,
            "grid_summary" : self.grid_summary,
        }
 
    def export_summary(self,grid_hour_path,daily_path,grid_path):
 
        if self.grid_summary is None or self.daily_summary is None or self.grid_hour_df is None:
            raise ValueError("The Dataframe is Empty")
 
        self.grid_hour_df.to_csv(grid_hour_path,index = False)
        self.grid_summary.to_csv(grid_path,index = False)
        self.daily_summary.to_csv(daily_path,index = False)
 
        logging.info("Succesfully Exported the summary")
        return {
            "grid_hour_path" : grid_hour_path,
            "grid_summary" : grid_path,
            "daily_summary" : daily_path
        }
def run_unit_validations():
        processor = UsageProcessor(
        dataframe = pd.DataFrame({
            "datetime" : ["2023-11-01 00:00:00"],
            "CellID" : [1],
            "countrycode" : [1],
            "smsin" : [0.34],
            "smsout" : [0.54],
            "callin" : [0.43],
            "callout" : [0.67],
            "internet" : [1.23]
        })
    )
 
        df = processor.load_data()
        assert len(df) == 1
        df = processor.clean_data()
        assert "timestamp"  in df
        df = processor.derive_time_features()
        assert "date" in df
        assert "hour" in df
        assert "day_of_week" in df
    
        grid_hour_df = processor.aggregate_to_grid_time()
        assert grid_hour_df.iloc[0]["sms_in"] == 0.34
        assert grid_hour_df.iloc[0]["sms_out"] == 0.54
    
        grid_hour_df = processor.derive_activity_features()
        assert "total_sms" in grid_hour_df
        assert "total_activity" in grid_hour_df
    
        result = processor.compute_kpis()
    
        assert result["daily_summary"].iloc[0]["unique_grids"] == 1
        assert result["grid_summary"].iloc[0]["active_hours"] == 1
    
        logger.info("All unit validations are done")
    
    
 
 
if __name__ == "__main__":
    print("#"*69)
    print("Run unit validation")
    print("#"*69)
    run_unit_validations()
    print("#"*69)
    print("Run UsageProcessor")
    print("#"*69)
    up = UsageProcessor(file_path = "../dataset/raw/sms-call-internet-mi-2013-11-01.csv")
    up.load_data()
    up.clean_data()
    up.derive_time_features()
    up.aggregate_to_grid_time()
    up.derive_activity_features()
    up.compute_kpis()
    up.export_summary("../dataset/landing/grid_hour.csv","../dataset/landing/grid_summary.csv","../dataset/landing/daily_summary.csv")
 