#!/usr/bin/env python
# coding: utf-8

import argparse
import pickle
from pathlib import Path

import pandas as pd
import xgboost as xgb

from sklearn.feature_extraction import DictVectorizer
from sklearn.metrics import root_mean_squared_error
import mlflow

from datetime import timedelta, datetime
from prefect import flow, task
from prefect.schedules import Interval


mlflow.set_tracking_uri(f"http://127.0.0.1:1994/")
mlflow.set_experiment("nyc-taxi-experiment")

models_folder = Path('models')
models_folder.mkdir(exist_ok=True)

@task(name="read_dataframe")
def read_dataframe(year, month):
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet'
    df = pd.read_parquet(url)

    df['duration'] = df.lpep_dropoff_datetime - df.lpep_pickup_datetime
    df.duration = df.duration.apply(lambda td: td.total_seconds() / 60)

    df = df[(df.duration >= 1) & (df.duration <= 60)]

    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].astype(str)

    df['PU_DO'] = df['PULocationID'] + '_' + df['DOLocationID']

    return df

@task(name='create_feature_matrix')
def create_X(df, dv=None):
    categorical = ['PU_DO']
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
        train = xgb.DMatrix(X_train, label=y_train)
        valid = xgb.DMatrix(X_val, label=y_val)

        best_params = {
            'learning_rate': 0.09585355369315604,
            'max_depth': 30,
            'min_child_weight': 1.060597050922164,
            'objective': 'reg:linear',
            'reg_alpha': 0.018060244040060163,
            'reg_lambda': 0.011658731377413597,
            'seed': 42
        }

        mlflow.log_params(best_params)

        booster = xgb.train(
            params=best_params,
            dtrain=train,
            num_boost_round=30,
            evals=[(valid, 'validation')],
            early_stopping_rounds=50
        )

        y_pred = booster.predict(valid)
        rmse = root_mean_squared_error(y_val, y_pred)
        mlflow.log_metric("rmse", rmse)

        with open(f"{models_folder}/preprocessor.b", "wb") as f_out:
            pickle.dump(dv, f_out)
        mlflow.log_artifact(f"{models_folder}/preprocessor.b", artifact_path="preprocessor")

        mlflow.xgboost.log_model(booster, artifact_path="models_mlflow")

        return run.info.run_id

@flow(name="main")
def execute(year: int=2021, month: int=1):
    #models_folder = set_tracking_directory(port=args.port, experiment=args.experiment)
    df_train = read_dataframe(year=year, month=month)

    next_year = year if month < 12 else year + 1
    next_month = month + 1 if month < 12 else 1
    df_val = read_dataframe(year=next_year, month=next_month)

    X_train, dv = create_X(df_train)
    X_val, _ = create_X(df_val, dv)

    y_train = df_train['duration'].values
    y_val = df_val['duration'].values

    run_id = train_model(X_train, y_train, X_val, y_val, dv, models_folder)

    with open("run_id.txt", "w") as f:
        f.write(run_id)

    print(f"MLflow run_id: {run_id}")
    return run_id


if __name__ == "__main__":
    

    parser = argparse.ArgumentParser(description='Train a model to predict taxi trip duration.')
    parser.add_argument('--year', type=int, default=2025, help='Year of the data to train on')
    parser.add_argument('--month', type=int, default=1, help='Month of the data to train on')
    parser.add_argument('--port', type=int, default=5000, help='Port for MLflow tracking server')
    parser.add_argument('--experiment', type=str, default='nyc-taxi-experiment', help='MLflow experiment name')
    args = parser.parse_args()

    schedule = Interval(
        timedelta(minutes=5),
        anchor_date=datetime.now(),
        timezone="America/Chicago"
        )
    
    execute.serve(
        name="orchestrate-duration-prediction",
        schedule=schedule,
        parameters={"year": args.year, "month": args.month}
        )

    