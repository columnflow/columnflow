# coding: utf-8

import unittest

from columnflow.inference import (
    InferenceModel, ParameterType, ParameterTransformation, ParameterTransformations, FlowStrategy,
)
from columnflow.util import DotDict


class TestInferenceModel(unittest.TestCase):

    def test_process_spec(self):
        # Test data
        name = "test_process"
        config_name = "test_config"
        config_process = "test_config_process"
        is_signal = True
        config_mc_datasets = ["dataset1", "dataset2"]
        scale = 2.0
        is_dynamic = True

        # Expected result
        expected_result = DotDict(
            name=name,
            is_signal=is_signal,
            config_data={
                config_name: DotDict(
                    process=config_process,
                    mc_datasets=config_mc_datasets,
                ),
            },
            scale=scale,
            is_dynamic=is_dynamic,
            parameters=[],
        )

        # Call the method
        result = InferenceModel.process_spec(
            name=name,
            is_signal=is_signal,
            config_data={
                config_name: InferenceModel.process_config_spec(
                    process=config_process,
                    mc_datasets=config_mc_datasets,
                ),
            },
            scale=scale,
            is_dynamic=is_dynamic,
        )

        self.assertDictEqual(result, expected_result)

    def test_category_spec(self):
        # Test data
        name = "test_category"
        config_name = "test_config"
        config_category = "test_config_category"
        config_variable = "test_config_variable"
        config_data_datasets = ["dataset1", "dataset2"]
        data_from_processes = ["process1", "process2"]
        mc_stats = (10, 0.1)
        empty_bin_value = 1e-4
        postfix = None
        rate_precision = 5

        # Expected result
        expected_result = DotDict(
            name=name,
            config_data={
                config_name: DotDict(
                    category=config_category,
                    variable=config_variable,
                    data_datasets=config_data_datasets,
                ),
            },
            data_from_processes=data_from_processes,
            flow_strategy=FlowStrategy.warn,
            mc_stats=mc_stats,
            postfix=postfix,
            empty_bin_value=empty_bin_value,
            rate_precision=rate_precision,
            processes=[],
        )

        # Call the method
        result = InferenceModel.category_spec(
            name=name,
            config_data={
                config_name: InferenceModel.category_config_spec(
                    category=config_category,
                    variable=config_variable,
                    data_datasets=config_data_datasets,
                ),
            },
            data_from_processes=data_from_processes,
            mc_stats=mc_stats,
            empty_bin_value=empty_bin_value,
            postfix=postfix,
            rate_precision=rate_precision,
        )

        self.assertDictEqual(result, expected_result)

    def test_parameter_spec(self):
        # Test data
        name = "test_parameter"
        type = ParameterType.rate_gauss
        transformations = [ParameterTransformation.centralize, ParameterTransformation.symmetrize]
        config_name = "test_config"
        config_shift_source = "test_shift_source"
        effect = 1.5
        effect_precision = 4

        # Expected result
        expected_result = DotDict(
            name=name,
            type=ParameterType.rate_gauss,
            transformations=ParameterTransformations(transformations),
            config_data={
                config_name: DotDict(
                    shift_source=config_shift_source,
                ),
            },
            effect=effect,
            effect_precision=effect_precision,
        )

        # Call the method
        result = InferenceModel.parameter_spec(
            name=name,
            type=type,
            transformations=transformations,
            config_data={
                config_name: InferenceModel.parameter_config_spec(
                    shift_source=config_shift_source,
                ),
            },
            effect=effect,
        )

        self.assertDictEqual(result, expected_result)

    def test_parameter_group_spec(self):
        # Test data
        name = "test_group"
        parameter_names = ["param1", "param2", "param3"]

        # Expected result
        expected_result = DotDict(
            name="test_group",
            parameter_names=["param1", "param2", "param3"],
        )

        # Call the method
        result = InferenceModel.parameter_group_spec(
            name=name,
            parameter_names=parameter_names,
        )

        # Assert the result
        self.assertDictEqual(result, expected_result)

    def test_parameter_group_spec_with_no_parameter_names(self):
        # Test data
        name = "test_group"

        # Expected result
        expected_result = DotDict(
            name="test_group",
            parameter_names=[],
        )

        # Call the method
        result = InferenceModel.parameter_group_spec(
            name=name,
        )

        self.assertDictEqual(result, expected_result)

    def test_require_shapes_for_parameter_shape(self):
        # No shape is required if the parameter type is a rate
        types = [ParameterType.rate_gauss, ParameterType.rate_uniform, ParameterType.rate_unconstrained]
        for t in types:
            with self.subTest(t=t):
                param_obj = DotDict(
                    type=t,
                    transformations=ParameterTransformations([ParameterTransformation.effect_from_rate]),
                    name="test_param",
                )
                result = InferenceModel.require_shapes_for_parameter(param_obj)
                self.assertFalse(result)

                # if the transformation is shape-based expect True
                param_obj.transformations = ParameterTransformations([ParameterTransformation.effect_from_shape])
                result = InferenceModel.require_shapes_for_parameter(param_obj)
                self.assertTrue(result)

        # No shape is required if the transformation is from a rate
        param_obj = DotDict(
            type=ParameterType.shape,
            transformations=ParameterTransformations([ParameterTransformation.effect_from_rate]),
            name="test_param",
        )
        result = InferenceModel.require_shapes_for_parameter(param_obj)
        self.assertFalse(result)

        param_obj.transformations = ParameterTransformations([ParameterTransformation.effect_from_shape])
        result = InferenceModel.require_shapes_for_parameter(param_obj)
        self.assertTrue(result)
