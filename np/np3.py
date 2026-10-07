import pandas as pd
 
ACTIVITY_FLOOR = 100
SPIKE_RATIO = 1.5
DROP_RATIO = 0.5
HIGH_ACTIVITY_RATIO = 1.5
 
 
def leave_out_one_median(values):

    original_index=values.index
 
    values = values.reset_index(drop = True)
 
    result = []
 
    for index in range(len(values)):
 
        remaining_values = values.drop(index)
 
        if len(remaining_values) == 0:
            result.append(pd.NA)
 
        else:
            result.append(remaining_values.median())
 
    return pd.Series(
        result,
        index = original_index
    )
 
 
grid_hour = pd.read_csv("../dataset/landing/grid_hour.csv")
 
grid_hour["date"] = pd.to_datetime(grid_hour["date"],errors = "coerce")
grid_hour["timestamp"] = grid_hour["date"] + pd.to_timedelta( grid_hour["hour"],unit="h")
 
grid_hour = grid_hour.sort_values([
    "grid_id","timestamp"
]).reset_index(drop = True)
 
daily_totals = (grid_hour.groupby(["date","grid_id"],as_index = False)["total_activity"]
             .sum()
             .rename(
                 columns = {
                    "total_activity" : "daily_total_activity"
                 }
             )
           )
 
grid_hour = grid_hour.merge(
    daily_totals,
    on = ["date","grid_id"],
    how = "left"
)
 
grid_hour["baseline_activity"] = (grid_hour.groupby(["date","grid_id"])
            ["total_activity"].transform(leave_out_one_median)
)
 
grid_hour["previous_activity"] = grid_hour.groupby(["date","grid_id"])["total_activity"].shift(1)
 
eligible_mask = grid_hour["total_activity"] >= ACTIVITY_FLOOR
 
 
alerts = []
 
for ind ,row in grid_hour.iterrows():
 
    if not eligible_mask.iloc[ind] :
        continue
 
    current_activity = row["total_activity"]
    baseline_activity = row["baseline_activity"]
    grid_id = row["grid_id"]
    timestamp = row["timestamp"]
 
    if (current_activity >= HIGH_ACTIVITY_RATIO * baseline_activity):
       
        alerts.append({
            "grid_id":grid_id,
            "timestamp" : timestamp,
            "alert_type" : "High Activity",
            "current_activity" : current_activity,
            "baseline_activity" : baseline_activity,
            "reason" : f"Current activity is {current_activity/baseline_activity :.2f}x times the within baseline"
        })
 
    if(pd.notna(row["previous_activity"]) and
       current_activity >= SPIKE_RATIO * row["previous_activity"]):
 
         alerts.append({
            "grid_id":grid_id,
            "timestamp" : timestamp,
            "alert_type" : "Activity Spike",
            "current_activity" : current_activity,
            "baseline_activity" : baseline_activity,
            "reason" : f"Current activity is {current_activity/row["previous_activity"]:.2f}x times the previous activity"
        })
 
    if(current_activity <= DROP_RATIO * baseline_activity):
 
        alerts.append({
            "grid_id":grid_id,
            "timestamp" : timestamp,
            "alert_type" : "Activity Drop",
            "current_activity" : current_activity,
            "baseline_activity" : baseline_activity,
            "reason" : f"Current activity is {current_activity/baseline_activity:.2f}x times less than the previous activity"
        })
 
alerts_df = pd.DataFrame(
    alerts,
    columns=[
        "grid_id",
        "timestamp",
        "alert_type",
        "current_activity",
        "baseline_activity",
        "reason"
    ]
)
 
alerts_df.to_csv("../dataset/landing/alerts_df.csv",index = False)
 
# Short Operational Summary
 
 
alert_by_type = alerts_df["alert_type"].value_counts()
 
if len(alert_by_type) > 0:
    print(alert_by_type)
else:
    print("No alerts")
 
alert_top10 = alerts_df["grid_id"].value_counts().head(10)
 
 
if len(alert_top10) > 0:
    print(alert_top10)
else:
    print("No alerts")
 
total_grid_hours = len(grid_hour)
 
 
if total_grid_hours > 0 :
 
    alert_proportion = (alerts_df[["grid_id","timestamp"]]
                        .drop_duplicates().shape[0] / total_grid_hours)
 
    print(alert_proportion)
 
 
# ============================================================
# ACTIVITY 9
# LIMITATIONS / FALSE POSITIVE STATEMENT
# ============================================================
 
print()
print("=" * 60)
print("BASELINE LIMITATIONS")
print("=" * 60)
 
print(
    """
The rule-based baseline identifies unusual activity relative
to the same grid's activity during the same day. It does not
determine the cause of the unusual activity.
 
Possible false positives include planned events, holidays,
maintenance activity, temporary traffic shifts, changes in
customer behaviour, or other legitimate network events.
 
The activity-only baseline cannot distinguish between a genuine
network fault, a planned event, unusual customer behaviour,
a special event, or a data-quality issue.
 
It also does not use weather, outages, alarms, topology,
network configuration, customer segments, historical days,
or external events. Therefore, an alert is an investigation
signal and not a diagnosis.
"""
)
 
print("=" * 60)