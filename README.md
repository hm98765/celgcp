
# CEL evaluator for GCP

**This is not an official Google product.**

This module allows to evaluate the Google Cloud [IAM Conditions](https://cloud.google.com/iam/docs/conditions-overview) where [CEL](https://cloud.google.com/iam/docs/conditions-overview#cel) is the expression language.

### Author
hm98765@github

### Usage

1. Install the package 
    ```bash
    pip install celgcp
    ```
    or
    ```
    poetry add celgcp
    ```
2. Import the class and exception
    ```python
    from celgcp.celgcp import CELEvaluator, CELEvaluatorException
    ```
3. Create a CEL Expression
    ```python
    cel_source = """
    resource.matchTag('prj/dataset', 'value_1') 
    && resource.name.startsWith('projects/my-project/datasets/foo')
    """
    ```
4. Create an instance of the CELEvaluator
    ```python
    cel_evaluator = CELEvaluator(cel_source)
    ```
5. Create the activation context
    ```python
     activation = {
        "resource": celpy.json_to_cel(
            {
                "name": "projects/my-project/datasets/foo/bar",
                "Tags": [
                    {"prj/dataset": "value_1"},
                    {"prj/table": "value_2"},
                    {"prj/mytag": "value_38"},
                    {"tagKeys/123456789012": "tagValues/567890123456"},
                    {"tagKeys/987654321": "tagValues/111111"},
                ],
            },
        ),
    }
    ```
6. call the evaluate method
    ```python
    result = cel_evaluator.evaluate(activation)
    ```

### Example
A complete example

```python

    from datetime import datetime
    import celpy
    from celgcp.celgcp import CELEvaluator, CELEvaluatorException

    cel_source = """
    resource.matchTag('prj/dataset', 'value_1') 
    && resource.name.startsWith('projects/my-project/datasets/foo')
    && request.time < timestamp("2024-03-21T01:14:51Z")
    """

    date_string = "2021-03-21T01:14:51Z"
    datetime_object = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ")

    activation = {
        "request": celpy.json_to_cel({"time": datetime_object}),
        "resource": celpy.json_to_cel(
            {
                "name": "projects/my-project/datasets/foo/bar",
                "Tags": [
                    {"prj/dataset": "value_1"},
                    {"prj/table": "value_2"},
                    {"prj/mytag": "value_38"},
                    {"tagKeys/123456789012": "tagValues/567890123456"},
                    {"tagKeys/987654321": "tagValues/111111"},
                ],
            },
        ),
    }

    cel_evaluator = CELEvaluator(cel_source)
    result = cel_evaluator.evaluate(activation)

```

## Language
- [Python](https://www.python.org/)

## Dependencies
The dependencies are listed in the pyproject.toml

## License
Apache 2.0; see [LICENSE](LICENSE) for details.


## Tests
poetry run pytest ./tests/tests.py

This is not an officially supported Google product. This project is not
eligible for the [Google Open Source Software Vulnerability Rewards
Program](https://bughunters.google.com/open-source-security).