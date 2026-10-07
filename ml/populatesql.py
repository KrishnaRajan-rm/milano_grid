import pandas as pd
import pymysql

connection = pymysql.connect(
    host="localhost",
    port=3306,
    user="root",
    password="root",
    database="telecom_analytics",
    autocommit=True
)

cursor = connection.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS ml2_features (
        grid_id INT NOT NULL,
        feature_timestamp DATETIME NOT NULL,
        avg_activity DOUBLE,
        activity_growth DOUBLE,
        active_hours BIGINT,
        peak_ratio DOUBLE,
        variability DOUBLE,
        internet_share DOUBLE,

        PRIMARY KEY (grid_id, feature_timestamp)
    )
""")

print("ML2 feature table ready")

features = pd.read_parquet(
    "../dataset/output/ml2_features"
)

print(f"Total feature rows: {len(features)}")

batch_size = 5000

for start in range(0, len(features), batch_size):

    batch = features.iloc[start:start + batch_size]

    rows = batch[
        [
            "grid_id",
            "feature_timestamp",
            "avg_activity",
            "activity_growth",
            "active_hours",
            "peak_ratio",
            "variability",
            "internet_share"
        ]
    ].itertuples(index=False, name=None)

    cursor.executemany("""
        INSERT INTO ml2_features (
            grid_id,
            feature_timestamp,
            avg_activity,
            activity_growth,
            active_hours,
            peak_ratio,
            variability,
            internet_share
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)

        ON DUPLICATE KEY UPDATE
            avg_activity = VALUES(avg_activity),
            activity_growth = VALUES(activity_growth),
            active_hours = VALUES(active_hours),
            peak_ratio = VALUES(peak_ratio),
            variability = VALUES(variability),
            internet_share = VALUES(internet_share)
    """, list(rows))

    connection.commit()

    print(
        f"Inserted rows {start + 1} "
        f"to {min(start + batch_size, len(features))}"
    )

cursor.close()
connection.close()

print("ML2 features successfully pushed to MySQL")