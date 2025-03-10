import unittest
from columnflow.inference import (
    InferenceModel, ParameterType, ParameterTransformation, ParameterTransformations,
    FlowStrategy,
)
from columnflow.util import DotDict


class TestInferenceModel(unittest.TestCase):

    def test_process_spec(self):
        # Test data
        name = "test_process"
        config_process = "test_config_process"
        is_signal = True
        config_mc_datasets = ["dataset1", "dataset2"]
        scale = 2.0
        is_dynamic = True

        # Expected result
        expected_result = DotDict([
            ("name", "test_process"),
            ("is_signal", True),
            ("config_process", "test_config_process"),
            ("config_mc_datasets", ["dataset1", "dataset2"]),
            ("scale", 2.0),
            ("parameters", []),
            ("is_dynamic", True),
        ])

        # Call the method
        result = InferenceModel.process_spec(
            name=name,
            config_process=config_process,
            is_signal=is_signal,
            config_mc_datasets=config_mc_datasets,
            scale=scale,
            is_dynamic=is_dynamic,
        )

        # Assert the result
        self.assertEqual(result, expected_result)

    def test_category_spec(self):
        # Test data
        name = "test_category"
        config_category = "test_config_category"
        config_variable = "test_config_variable"
        config_data_datasets = ["dataset1", "dataset2"]
        data_from_processes = ["process1", "process2"]
        mc_stats = (10, 0.1)
        empty_bin_value = 1e-4

        # Expected result
        expected_result = DotDict([
            ("name", "test_category"),
            ("config_category", "test_config_category"),
            ("config_variable", "test_config_variable"),
            ("config_data_datasets", ["dataset1", "dataset2"]),
            ("data_from_processes", ["process1", "process2"]),
            ("flow_strategy", FlowStrategy.warn),
            ("mc_stats", (10, 0.1)),
            ("empty_bin_value", 1e-4),
            ("processes", []),
        ])

        # Call the method
        result = InferenceModel.category_spec(
            name=name,
            config_category=config_category,
            config_variable=config_variable,
            config_data_datasets=config_data_datasets,
            data_from_processes=data_from_processes,
            mc_stats=mc_stats,
            empty_bin_value=empty_bin_value,
        )

        # Assert the result
        self.assertEqual(result, expected_result)

    def test_parameter_spec(self):
        # Test data
        name = "test_parameter"
        type = ParameterType.rate_gauss
        transformations = [ParameterTransformation.centralize, ParameterTransformation.symmetrize]
        config_shift_source = "test_shift_source"
        effect = 1.5

        # Expected result
        expected_result = DotDict([
            ("name", "test_parameter"),
            ("type", ParameterType.rate_gauss),
            ("transformations", ParameterTransformations(transformations)),
            ("config_shift_source", "test_shift_source"),
            ("effect", 1.5),
        ])

        # Call the method
        result = InferenceModel.parameter_spec(
            name=name,
            type=type,
            transformations=transformations,
            config_shift_source=config_shift_source,
            effect=effect,
        )

        # Assert the result
        self.assertEqual(result, expected_result)

    def test_parameter_spec_with_default_transformations(self):
        # Test data
        name = "test_parameter"
        type = ParameterType.rate_gauss
        config_shift_source = "test_shift_source"
        effect = 1.5

        # Expected result
        expected_result = DotDict([
            ("name", "test_parameter"),
            ("type", ParameterType.rate_gauss),
            ("transformations", ParameterTransformations([ParameterTransformation.none])),
            ("config_shift_source", "test_shift_source"),
            ("effect", 1.5),
        ])

        # Call the method
        result = InferenceModel.parameter_spec(
            name=name,
            type=type,
            config_shift_source=config_shift_source,
            effect=effect,
        )

        # Assert the result
        self.assertEqual(result, expected_result)

    def test_parameter_spec_with_string_type_and_transformations(self):
        # Test data
        name = "test_parameter"
        type = "rate_gauss"
        transformations = ["centralize", "symmetrize"]
        config_shift_source = "test_shift_source"
        effect = 1.5

        # Expected result
        expected_result = DotDict([
            ("name", "test_parameter"),
            ("type", ParameterType.rate_gauss),
            ("transformations", ParameterTransformations([
                ParameterTransformation.centralize,
                ParameterTransformation.symmetrize,
            ])),
            ("config_shift_source", "test_shift_source"),
            ("effect", 1.5),
        ])

        # Call the method
        result = InferenceModel.parameter_spec(
            name=name,
            type=type,
            transformations=transformations,
            config_shift_source=config_shift_source,
            effect=effect,
        )

        # Assert the result
        self.assertEqual(result, expected_result)

    def test_parameter_group_spec(self):
        # Test data
        name = "test_group"
        parameter_names = ["param1", "param2", "param3"]

        # Expected result
        expected_result = DotDict([
            ("name", "test_group"),
            ("parameter_names", ["param1", "param2", "param3"]),
        ])

        # Call the method
        result = InferenceModel.parameter_group_spec(
            name=name,
            parameter_names=parameter_names,
        )

        # Assert the result
        self.assertEqual(result, expected_result)

    def test_parameter_group_spec_with_no_parameter_names(self):
        # Test data
        name = "test_group"

        # Expected result
        expected_result = DotDict([
            ("name", "test_group"),
            ("parameter_names", []),
        ])

        # Call the method
        result = InferenceModel.parameter_group_spec(
            name=name,
        )

        # Assert the result
        self.assertEqual(result, expected_result)

    def test_require_shapes_for_parameter_shape(self):
        # No shape is required if the parameter type is a rate
        types = [ParameterType.rate_gauss, ParameterType.rate_uniform, ParameterType.rate_unconstrained]
        for t in types:
            with self.subTest(t=t):
                param_obj = DotDict.wrap({
                    "type": t,
                    "transformations": ParameterTransformations([ParameterTransformation.effect_from_rate]),
                    "name": "test_param",
                })
                result = InferenceModel.require_shapes_for_parameter(param_obj)
                self.assertFalse(result)

                # if the transformation is shape-based expect True
                param_obj.transformations = ParameterTransformations([ParameterTransformation.effect_from_shape])
                result = InferenceModel.require_shapes_for_parameter(param_obj)
                self.assertTrue(result)

        # No shape is required if the transformation is from a rate
        param_obj = DotDict.wrap({
            "type": ParameterType.shape,
            "transformations": ParameterTransformations([ParameterTransformation.effect_from_rate]),
            "name": "test_param",
        })
        result = InferenceModel.require_shapes_for_parameter(param_obj)
        self.assertFalse(result)

        param_obj.transformations = ParameterTransformations([ParameterTransformation.effect_from_shape])
        result = InferenceModel.require_shapes_for_parameter(param_obj)
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
