# Copyright Shray15, 2024-
# https://github.com/Shray15/Boston_House_Pricing/blob/53837c94643531f93c0ff00b40a7fbef9793f17d/boston-house-kfp.py
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

from dewret.tasks import task, workflow
from dewret.data import DataManager, DatasetPath
from dewret.renderers.kubeflow import KFPDataset

DATASET_MANAGER = DataManager()

Artifact = DatasetPath
Dataset = KFPDataset[DatasetPath]

output_dataset: Artifact = DATASET_MANAGER.path()
out_data: Artifact = DATASET_MANAGER.path()
X_train_artifact: Dataset = DATASET_MANAGER.path()
X_test_artifact: Dataset = DATASET_MANAGER.path()
y_train_artifact: Dataset = DATASET_MANAGER.path()
y_test_artifact: Dataset = DATASET_MANAGER.path()
model_file: Artifact = DATASET_MANAGER.path()
metrics_output: Dataset = DATASET_MANAGER.path()
prediction: Dataset = DATASET_MANAGER.path()
X_test_scaled: Dataset = DATASET_MANAGER.path()


@task()
def load_dataset_from_gcs(bucket_name: str, blob_name: str) -> Dataset:
    import pandas as pd
    from minio import Minio
    import io

    client = Minio("minio-service.default:9000", "minio", "minio123", secure=False)
    response = client.get_object(bucket_name, blob_name)

    data = pd.read_csv(
        io.BytesIO(response.data), header=None, delim_whitespace=True, comment="#"
    )
    data.to_csv(output_dataset, header=True, index=False)

    return output_dataset


@task()
def preprocess_the_dataset(dataset_content: Dataset) -> Dataset:
    import pandas as pd

    data = pd.read_csv(dataset_content, header=0)
    if data.isna().sum().any():
        raise ValueError("The data needs preprocessing (remove missing values)")

    data.to_csv(out_data, index=False)
    return out_data


@task()
def train_test_split(
    input_df: Dataset,
) -> tuple[Dataset, Dataset, Dataset, Dataset]:
    from sklearn.model_selection import train_test_split
    import pandas as pd

    df = pd.read_csv(input_df)
    X = df.iloc[:, :-1]
    y = df.iloc[:, -1]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42
    )

    X_train.to_csv(X_train_artifact, index=False)
    X_test.to_csv(X_test_artifact, index=False)
    y_train.to_csv(y_train_artifact, index=False)
    y_test.to_csv(y_test_artifact, index=False)

    return (
        X_train_artifact,
        X_test_artifact,
        y_train_artifact,
        y_test_artifact,
    )


@task()
def model_training(
    X_train_input: Dataset,
    X_test_input: Dataset,
    y_train_input: Dataset,
) -> tuple[Dataset, Artifact]:
    from sklearn.preprocessing import StandardScaler
    from sklearn.linear_model import LinearRegression
    import joblib
    from minio import Minio
    import pandas as pd

    scaler = StandardScaler()

    X_train = pd.read_csv(X_train_input)
    X_test = pd.read_csv(X_test_input)
    y_train = pd.read_csv(y_train_input)

    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled2 = pd.DataFrame(scaler.transform(X_test))
    X_test_scaled2.to_csv(X_test_scaled, index=False)  # Fixing typo here

    regression = LinearRegression()
    regression.fit(X_train_scaled, y_train)

    joblib.dump(regression, model_file)
    client = Minio("minio-service.default:9000", "minio", "minio123", secure=False)
    client.fput_object("boston-house-data", "data/model.pkl", str(model_file))
    return X_test_scaled, model_file


@task()
def predict(X_test: Dataset, trained_model: Artifact) -> Dataset:
    import joblib
    import pandas as pd
    from minio import Minio

    X_test_data = pd.read_csv(X_test)

    client = Minio("minio-service.default:9000", "minio", "minio123", secure=False)
    model_file = "model.pkl"
    client.fget_object("boston-house-data", "data/model.pkl", model_file)
    regression = joblib.load(model_file)

    predictions = regression.predict(X_test_data)
    pd.DataFrame(predictions).to_csv(prediction, index=False)
    return prediction


@task()
def evaluate(y_test: Dataset, predictions: Dataset) -> Dataset:
    from sklearn.metrics import mean_squared_error, mean_absolute_error
    import pandas as pd
    import numpy as np

    y_test_data = pd.read_csv(y_test)
    predictions_data = pd.read_csv(predictions)

    mae = mean_absolute_error(y_test_data, predictions_data)
    mse = mean_squared_error(y_test_data, predictions_data)
    rmse = np.sqrt(mse)

    with open(metrics_output, "w") as f:
        f.write(f"MAE: {mae}\n")
        f.write(f"MSE: {mse}\n")
        f.write(f"RMSE: {rmse}\n")

    return metrics_output


@workflow()
def pipeline() -> Dataset:
    read_data = load_dataset_from_gcs(
        bucket_name="boston-house-data", blob_name="data/housing.csv"
    )
    out_data = preprocess_the_dataset(dataset_content=read_data)
    X_train_artifact, X_test_artifact, y_train_artifact, y_test_artifact = (
        train_test_split(input_df=out_data)
    )
    X_test_scaled, trained_model = model_training(
        X_train_input=X_train_artifact,
        X_test_input=X_test_artifact,
        y_train_input=y_train_artifact,
    )  # Fixed typo here
    predicted_value = predict(X_test=X_test_scaled, trained_model=trained_model)
    return evaluate(
        y_test=y_test_artifact, predictions=predicted_value
    )  # Fixed typo here
