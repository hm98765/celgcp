"""
 Copyright 2024 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 """

from datetime import datetime
import celpy
import pytest
from celgcp.celgcp import CELEvaluator, CELEvaluatorException


def test_cel_resource_name_and_request_time():
    cel_source = """
    resource.name.startsWith('projects/my-project/datasets/foo')
    && request.time < timestamp("2024-03-21T01:14:51Z")
    """

    date_string = "2021-03-21T01:14:51Z"
    datetime_object = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ")

    activation = {
        "request": celpy.json_to_cel({"time": datetime_object}),
        "resource": celpy.json_to_cel(
            {
                "name": "projects/my-project/datasets/foo/bar",
            },
        ),
    }
    cel_evaluator = CELEvaluator(cel_source)
    result = cel_evaluator.evaluate(activation)
    assert result == True, "This eval should be True"


def test_cel_resource_name_and_request_time_matchtag_missing_exception():
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
            },
        ),
    }
    cel_evaluator = CELEvaluator(cel_source)
    with pytest.raises(CELEvaluatorException):
        cel_evaluator.evaluate(activation)



def test_cel_matchtag_resource_requesttime():
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
    assert result == True, "This eval should be True"


def test_cel_complex():
    cel_source = """
    resource.matchTag('prj/dataset', 'value_1') 
    && (resource.matchTag('prj/table', 'value_2')  || resource.matchTag('prj/mytag', 'value_3'))
    && resource.name.startsWith('projects/my-project/datasets/foo')
    && request.time < timestamp("2024-03-21T01:14:51Z")
    && resource.matchTagId('tagKeys/123456789012', 'tagValues/567890123456')
    && resource.hasTagKeyId('tagKeys/987654321')
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
    assert result == True, "This eval should be True"
