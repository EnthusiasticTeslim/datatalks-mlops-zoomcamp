import pandas as pd
import mlflow, sklearn, pickle
from pathlib import Path

from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import root_mean_squared_error, r2_score, mean_absolute_error

from datetime import timedelta, datetime
from prefect import flow, task
from prefect.schedules import Interval


mlflow.set_tracking_uri(f"http://127.0.0.1:1994/")
mlflow.set_experiment("nyc-yellow-taxi-Mar2023-experiment")

models_folder = Path('models')
models_folder.mkdir(exist_ok=True)


@task(name="read_dataframe")
def read_dataframe(filename):
    df = pd.read_parquet(filename)

    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df.duration = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)]

    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].astype(str)
    
    return df


@task(name='split_data')
def train_val_split(df, train_size=0.8):
    train_size = int(len(df) * train_size)
    train = df[:train_size]
    val = df[train_size:]
    return train, val

@task(name='create_feature_matrix')
def create_X(df, dv=None):
    categorical = ['PULocationID', 'DOLocationID']
    numerical = ['trip_distance']
    dicts = df[categorical + numerical].to_dict(orient='records')

    if dv is None:
        dv = DictVectorizer(sparse=True)
        X = dv.fit_transform(dicts)
    else:
        X = dv.transform(dicts)

    return X, dv

@task(name='train_model')
def train_model(X_train, y_train, X_val, y_val, dv, models_folder):

    with mlflow.start_run() as run:
        # set tag as linear regression model
        mlflow.set_tag("model", "linear_regression")
        mlflow.set_tag("version", f"{sklearn.__version__}")

        model = LinearRegression()
        model.fit(X_train, y_train)

        mlflow.log_params({
            "n_features": X_train.shape[1],
            "n_targets": y_train.shape[1] if len(y_train.shape) > 1 else 1,
            "n_samples": X_train.shape[0],
            "intercept_": model.intercept_
        })

        y_pred = model.predict(X_val)
        rmse = root_mean_squared_error(y_val, y_pred)
        r2 = r2_score(y_val, y_pred)
        mae = mean_absolute_error(y_val, y_pred)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("mae", mae)


        with open(f"{models_folder}/featurizer.b", "wb") as f_out:
            pickle.dump(dv, f_out)
        mlflow.log_artifact(f"{models_folder}/featurizer.b", artifact_path="featurizer")

        mlflow.sklearn.log_model(model, artifact_path="models_mlflow")

        return run.info.run_id


@flow(name="main")
def execute(file_path = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet"):
    # load the parquet file
    df = read_dataframe(filename=file_path)
    # split the data into train and validation sets
    df_train, df_val = train_val_split(df)
    # create feature matrix
    X_train, dv = create_X(df_train)
    X_val, _ = create_X(df_val, dv)
    # get the target variable
    y_train = df_train['duration'].values
    y_val = df_val['duration'].values
    # train the model
    run_id = train_model(X_train, y_train, X_val, y_val, dv, models_folder)

    with open("run_id.txt", "w") as f:
        f.write(run_id)

    print(f"MLflow run_id: {run_id}")
    return run_id


if __name__ == "__main__":

    schedule = Interval(
        timedelta(minutes=5),
        anchor_date=datetime.now(),
        timezone="America/Chicago"
        )
    
    execute.serve(
        name="yellow-March23-Taxi-forecast",
        schedule=schedule 
        )