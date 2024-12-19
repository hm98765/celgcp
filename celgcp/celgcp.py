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

import celpy

class CELEvaluatorException(Exception):
    pass

class CELEvaluator:
    """
    This class is customized to perform the evaluation of CEL expressions in the context of Google Cloud Platform
    """
    def __init__(self, cel_source):
        decls = {
            "resource": celpy.celtypes.StringType,
            "matchTag": celpy.celtypes.FunctionType,
            "matchTagId": celpy.celtypes.FunctionType,
            "hasTagKeyId": celpy.celtypes.FunctionType,
            "request": celpy.celtypes.StringType,
        }

        my_functions = {}
        my_functions["matchTag"] = self.matchTag
        my_functions["matchTagId"] = self.matchTagId
        my_functions["hasTagKeyId"] = self.hasTagKeyId

        env = celpy.Environment(annotations=decls)
        ast = env.compile(cel_source)

        self.prgm = env.program(ast, functions=my_functions)

    def matchTag(
        self,
        resource: celpy.celtypes.StringType,
        key: celpy.celtypes.StringType,
        value: celpy.celtypes.StringType,
    ) -> celpy.celtypes.BoolType:

        if "Tags" not in resource:
            raise CELEvaluatorException("Tags should be a key in resource ")

        tags = resource["Tags"]
        for tag in tags:
            if key in tag:
                if tag[key] == value:
                    return celpy.celtypes.BoolType(bool(True))
        return celpy.celtypes.BoolType(bool(False))

    def matchTagId(
        self,
        resource: celpy.celtypes.StringType,
        key: celpy.celtypes.StringType,
        value: celpy.celtypes.StringType,
    ) -> celpy.celtypes.BoolType:

        return self.matchTag(resource, key, value)

    def hasTagKeyId(
        self,
        resource: celpy.celtypes.StringType,
        key: celpy.celtypes.StringType,
    ) -> celpy.celtypes.BoolType:

        tags = resource["Tags"]
        for tag in tags:
            if key in tag:
                return celpy.celtypes.BoolType(bool(True))

        return celpy.celtypes.BoolType(bool(False))

    def evaluate(self, activation) -> bool:
        return self.prgm.evaluate(activation)
