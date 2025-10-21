"""Comprehensive tests for Results and PipelineResult classes."""

from cleared.engine import Results, PipelineResult


class TestPipelineResult:
    """Test the PipelineResult dataclass."""

    def test_initialization_with_required_fields(self):
        """Test PipelineResult initialization with required fields only."""
        result = PipelineResult(status="success")

        assert result.status == "success"
        assert result.error is None

    def test_initialization_with_all_fields(self):
        """Test PipelineResult initialization with all fields."""
        result = PipelineResult(status="error", error="Test error message")

        assert result.status == "error"
        assert result.error == "Test error message"

    def test_initialization_with_skipped_status(self):
        """Test PipelineResult initialization with skipped status."""
        result = PipelineResult(status="skipped")

        assert result.status == "skipped"
        assert result.error is None

    def test_initialization_with_error_status_and_message(self):
        """Test PipelineResult initialization with error status and message."""
        error_msg = "Pipeline failed due to data validation error"
        result = PipelineResult(status="error", error=error_msg)

        assert result.status == "error"
        assert result.error == error_msg

    def test_initialization_with_success_status_and_error_message(self):
        """Test PipelineResult initialization with success status but error message (edge case)."""
        result = PipelineResult(status="success", error="Warning message")

        assert result.status == "success"
        assert result.error == "Warning message"

    def test_initialization_with_empty_error_message(self):
        """Test PipelineResult initialization with empty error message."""
        result = PipelineResult(status="error", error="")

        assert result.status == "error"
        assert result.error == ""

    def test_initialization_with_none_error_message(self):
        """Test PipelineResult initialization with None error message."""
        result = PipelineResult(status="error", error=None)

        assert result.status == "error"
        assert result.error is None

    def test_immutability_after_creation(self):
        """Test that PipelineResult fields can be modified after creation."""
        result = PipelineResult(status="success")

        # Should be able to modify fields (dataclass allows this)
        result.status = "error"
        result.error = "Modified error"

        assert result.status == "error"
        assert result.error == "Modified error"

    def test_string_representation(self):
        """Test string representation of PipelineResult."""
        result = PipelineResult(status="error", error="Test error")
        str_repr = str(result)

        assert "PipelineResult" in str_repr
        assert "error" in str_repr
        assert "Test error" in str_repr

    def test_equality_comparison(self):
        """Test equality comparison between PipelineResult instances."""
        result1 = PipelineResult(status="success", error=None)
        result2 = PipelineResult(status="success", error=None)
        result3 = PipelineResult(status="error", error="Test error")

        assert result1 == result2
        assert result1 != result3
        assert result2 != result3

    def test_dict_functionality(self):
        """Test that PipelineResult can be used as dictionary value."""
        result1 = PipelineResult(status="success")
        result2 = PipelineResult(status="error", error="Test error")

        # Create a dictionary using PipelineResult as values
        result_dict = {"success_key": result1, "error_key": result2}

        assert result_dict["success_key"] == result1
        assert result_dict["error_key"] == result2
        assert result_dict["success_key"].status == "success"
        assert result_dict["error_key"].status == "error"


class TestResults:
    """Test the Results dataclass."""

    def test_initialization_with_defaults(self):
        """Test Results initialization with default values."""
        results = Results()

        assert results.success is True
        assert results.results == {}
        assert results.execution_order == []

    def test_initialization_with_custom_values(self):
        """Test Results initialization with custom values."""
        pipeline_result = PipelineResult(status="success")
        results = Results(
            success=False,
            results={"pipeline1": pipeline_result},
            execution_order=["pipeline1"],
        )

        assert results.success is False
        assert len(results.results) == 1
        assert "pipeline1" in results.results
        assert results.results["pipeline1"] == pipeline_result
        assert results.execution_order == ["pipeline1"]

    def test_add_pipeline_result_success(self):
        """Test adding a successful pipeline result."""
        results = Results()

        results.add_pipeline_result("pipeline1", "success")

        assert "pipeline1" in results.results
        assert results.results["pipeline1"].status == "success"
        assert results.results["pipeline1"].error is None

    def test_add_pipeline_result_error(self):
        """Test adding an error pipeline result."""
        results = Results()
        error_msg = "Pipeline failed due to data validation"

        results.add_pipeline_result("pipeline1", "error", error_msg)

        assert "pipeline1" in results.results
        assert results.results["pipeline1"].status == "error"
        assert results.results["pipeline1"].error == error_msg

    def test_add_pipeline_result_skipped(self):
        """Test adding a skipped pipeline result."""
        results = Results()

        results.add_pipeline_result("pipeline1", "skipped")

        assert "pipeline1" in results.results
        assert results.results["pipeline1"].status == "skipped"
        assert results.results["pipeline1"].error is None

    def test_add_pipeline_result_overwrite_existing(self):
        """Test that adding a pipeline result overwrites existing one."""
        results = Results()

        # Add initial result
        results.add_pipeline_result("pipeline1", "success")
        assert results.results["pipeline1"].status == "success"

        # Overwrite with error
        results.add_pipeline_result("pipeline1", "error", "New error")
        assert results.results["pipeline1"].status == "error"
        assert results.results["pipeline1"].error == "New error"

    def test_add_execution_order(self):
        """Test adding pipeline to execution order."""
        results = Results()

        results.add_execution_order("pipeline1")
        results.add_execution_order("pipeline2")
        results.add_execution_order("pipeline3")

        assert results.execution_order == ["pipeline1", "pipeline2", "pipeline3"]

    def test_add_execution_order_duplicates(self):
        """Test adding duplicate pipeline UIDs to execution order."""
        results = Results()

        results.add_execution_order("pipeline1")
        results.add_execution_order("pipeline2")
        results.add_execution_order("pipeline1")  # Duplicate

        assert results.execution_order == ["pipeline1", "pipeline2", "pipeline1"]

    def test_set_success_true(self):
        """Test setting success status to True."""
        results = Results(success=False)

        results.set_success(True)

        assert results.success is True

    def test_set_success_false(self):
        """Test setting success status to False."""
        results = Results(success=True)

        results.set_success(False)

        assert results.success is False

    def test_has_errors_no_results(self):
        """Test has_errors with no pipeline results."""
        results = Results()

        assert results.has_errors() is False

    def test_has_errors_all_success(self):
        """Test has_errors with all successful results."""
        results = Results()
        results.add_pipeline_result("pipeline1", "success")
        results.add_pipeline_result("pipeline2", "success")
        results.add_pipeline_result("pipeline3", "skipped")

        assert results.has_errors() is False

    def test_has_errors_with_errors(self):
        """Test has_errors with some error results."""
        results = Results()
        results.add_pipeline_result("pipeline1", "success")
        results.add_pipeline_result("pipeline2", "error", "Test error")
        results.add_pipeline_result("pipeline3", "skipped")

        assert results.has_errors() is True

    def test_has_errors_all_errors(self):
        """Test has_errors with all error results."""
        results = Results()
        results.add_pipeline_result("pipeline1", "error", "Error 1")
        results.add_pipeline_result("pipeline2", "error", "Error 2")

        assert results.has_errors() is True

    def test_get_error_count_no_results(self):
        """Test get_error_count with no pipeline results."""
        results = Results()

        assert results.get_error_count() == 0

    def test_get_error_count_no_errors(self):
        """Test get_error_count with no error results."""
        results = Results()
        results.add_pipeline_result("pipeline1", "success")
        results.add_pipeline_result("pipeline2", "success")
        results.add_pipeline_result("pipeline3", "skipped")

        assert results.get_error_count() == 0

    def test_get_error_count_with_errors(self):
        """Test get_error_count with some error results."""
        results = Results()
        results.add_pipeline_result("pipeline1", "success")
        results.add_pipeline_result("pipeline2", "error", "Error 1")
        results.add_pipeline_result("pipeline3", "skipped")
        results.add_pipeline_result("pipeline4", "error", "Error 2")

        assert results.get_error_count() == 2

    def test_get_error_count_all_errors(self):
        """Test get_error_count with all error results."""
        results = Results()
        results.add_pipeline_result("pipeline1", "error", "Error 1")
        results.add_pipeline_result("pipeline2", "error", "Error 2")
        results.add_pipeline_result("pipeline3", "error", "Error 3")

        assert results.get_error_count() == 3

    def test_get_successful_pipelines_no_results(self):
        """Test get_successful_pipelines with no pipeline results."""
        results = Results()

        assert results.get_successful_pipelines() == []

    def test_get_successful_pipelines_no_success(self):
        """Test get_successful_pipelines with no successful results."""
        results = Results()
        results.add_pipeline_result("pipeline1", "error", "Error 1")
        results.add_pipeline_result("pipeline2", "skipped")

        assert results.get_successful_pipelines() == []

    def test_get_successful_pipelines_with_success(self):
        """Test get_successful_pipelines with some successful results."""
        results = Results()
        results.add_pipeline_result("pipeline1", "success")
        results.add_pipeline_result("pipeline2", "error", "Error 1")
        results.add_pipeline_result("pipeline3", "success")
        results.add_pipeline_result("pipeline4", "skipped")

        successful = results.get_successful_pipelines()
        assert len(successful) == 2
        assert "pipeline1" in successful
        assert "pipeline3" in successful
        assert "pipeline2" not in successful
        assert "pipeline4" not in successful

    def test_get_successful_pipelines_all_success(self):
        """Test get_successful_pipelines with all successful results."""
        results = Results()
        results.add_pipeline_result("pipeline1", "success")
        results.add_pipeline_result("pipeline2", "success")
        results.add_pipeline_result("pipeline3", "success")

        successful = results.get_successful_pipelines()
        assert len(successful) == 3
        assert "pipeline1" in successful
        assert "pipeline2" in successful
        assert "pipeline3" in successful

    def test_get_failed_pipelines_no_results(self):
        """Test get_failed_pipelines with no pipeline results."""
        results = Results()

        assert results.get_failed_pipelines() == []

    def test_get_failed_pipelines_no_errors(self):
        """Test get_failed_pipelines with no error results."""
        results = Results()
        results.add_pipeline_result("pipeline1", "success")
        results.add_pipeline_result("pipeline2", "success")
        results.add_pipeline_result("pipeline3", "skipped")

        assert results.get_failed_pipelines() == []

    def test_get_failed_pipelines_with_errors(self):
        """Test get_failed_pipelines with some error results."""
        results = Results()
        results.add_pipeline_result("pipeline1", "success")
        results.add_pipeline_result("pipeline2", "error", "Error 1")
        results.add_pipeline_result("pipeline3", "success")
        results.add_pipeline_result("pipeline4", "error", "Error 2")

        failed = results.get_failed_pipelines()
        assert len(failed) == 2
        assert "pipeline2" in failed
        assert "pipeline4" in failed
        assert "pipeline1" not in failed
        assert "pipeline3" not in failed

    def test_get_failed_pipelines_all_errors(self):
        """Test get_failed_pipelines with all error results."""
        results = Results()
        results.add_pipeline_result("pipeline1", "error", "Error 1")
        results.add_pipeline_result("pipeline2", "error", "Error 2")
        results.add_pipeline_result("pipeline3", "error", "Error 3")

        failed = results.get_failed_pipelines()
        assert len(failed) == 3
        assert "pipeline1" in failed
        assert "pipeline2" in failed
        assert "pipeline3" in failed

    def test_complex_workflow_scenario(self):
        """Test a complex workflow scenario with multiple pipelines."""
        results = Results()

        # Add execution order
        results.add_execution_order("pipeline1")
        results.add_execution_order("pipeline2")
        results.add_execution_order("pipeline3")
        results.add_execution_order("pipeline4")

        # Add results
        results.add_pipeline_result("pipeline1", "success")
        results.add_pipeline_result("pipeline2", "error", "Data validation failed")
        results.add_pipeline_result("pipeline3", "skipped")
        results.add_pipeline_result("pipeline4", "success")

        # Set overall success to False due to pipeline2 error
        results.set_success(False)

        # Verify state
        assert results.success is False
        assert results.execution_order == [
            "pipeline1",
            "pipeline2",
            "pipeline3",
            "pipeline4",
        ]
        assert results.has_errors() is True
        assert results.get_error_count() == 1
        assert results.get_successful_pipelines() == ["pipeline1", "pipeline4"]
        assert results.get_failed_pipelines() == ["pipeline2"]

        # Verify individual results
        assert results.results["pipeline1"].status == "success"
        assert results.results["pipeline1"].error is None
        assert results.results["pipeline2"].status == "error"
        assert results.results["pipeline2"].error == "Data validation failed"
        assert results.results["pipeline3"].status == "skipped"
        assert results.results["pipeline3"].error is None
        assert results.results["pipeline4"].status == "success"
        assert results.results["pipeline4"].error is None

    def test_empty_string_pipeline_uid(self):
        """Test handling of empty string pipeline UID."""
        results = Results()

        results.add_pipeline_result("", "success")
        results.add_execution_order("")

        assert "" in results.results
        assert results.results[""].status == "success"
        assert results.execution_order == [""]

    def test_none_pipeline_uid(self):
        """Test handling of None pipeline UID."""
        results = Results()

        results.add_pipeline_result(None, "error", "None UID error")
        results.add_execution_order(None)

        assert None in results.results
        assert results.results[None].status == "error"
        assert results.results[None].error == "None UID error"
        assert results.execution_order == [None]

    def test_unicode_pipeline_uid(self):
        """Test handling of unicode pipeline UID."""
        results = Results()
        unicode_uid = "pipeline_🚀_test"

        results.add_pipeline_result(unicode_uid, "success")
        results.add_execution_order(unicode_uid)

        assert unicode_uid in results.results
        assert results.results[unicode_uid].status == "success"
        assert results.execution_order == [unicode_uid]

    def test_large_number_of_pipelines(self):
        """Test handling of large number of pipelines."""
        results = Results()
        num_pipelines = 1000

        # Add many pipelines
        for i in range(num_pipelines):
            uid = f"pipeline_{i}"
            status = "success" if i % 2 == 0 else "error"
            error = f"Error {i}" if status == "error" else None

            results.add_pipeline_result(uid, status, error)
            results.add_execution_order(uid)

        # Verify counts
        assert len(results.results) == num_pipelines
        assert len(results.execution_order) == num_pipelines
        assert results.get_error_count() == num_pipelines // 2
        assert len(results.get_successful_pipelines()) == num_pipelines // 2
        assert len(results.get_failed_pipelines()) == num_pipelines // 2

    def test_string_representation(self):
        """Test string representation of Results."""
        results = Results()
        results.add_pipeline_result("pipeline1", "success")
        results.add_pipeline_result("pipeline2", "error", "Test error")

        str_repr = str(results)

        assert "Results" in str_repr
        assert "pipeline1" in str_repr
        assert "pipeline2" in str_repr

    def test_equality_comparison(self):
        """Test equality comparison between Results instances."""
        results1 = Results()
        results1.add_pipeline_result("pipeline1", "success")
        results1.add_execution_order("pipeline1")

        results2 = Results()
        results2.add_pipeline_result("pipeline1", "success")
        results2.add_execution_order("pipeline1")

        results3 = Results()
        results3.add_pipeline_result("pipeline1", "error", "Different error")
        results3.add_execution_order("pipeline1")

        assert results1 == results2
        assert results1 != results3
        assert results2 != results3

    def test_immutability_after_creation(self):
        """Test that Results fields can be modified after creation."""
        results = Results()

        # Should be able to modify fields (dataclass allows this)
        results.success = False
        results.results["test"] = PipelineResult(status="error", error="Test")
        results.execution_order.append("test")

        assert results.success is False
        assert "test" in results.results
        assert "test" in results.execution_order


class TestResultsEdgeCases:
    """Test edge cases and error scenarios for Results class."""

    def test_add_pipeline_result_with_invalid_status(self):
        """Test adding pipeline result with invalid status (should still work due to type hints)."""
        results = Results()

        # This should work due to Python's dynamic typing, but type checkers would warn
        results.add_pipeline_result("pipeline1", "invalid_status")

        assert "pipeline1" in results.results
        assert results.results["pipeline1"].status == "invalid_status"

    def test_add_pipeline_result_with_none_status(self):
        """Test adding pipeline result with None status."""
        results = Results()

        results.add_pipeline_result("pipeline1", None)

        assert "pipeline1" in results.results
        assert results.results["pipeline1"].status is None

    def test_add_pipeline_result_with_empty_error(self):
        """Test adding pipeline result with empty error message."""
        results = Results()

        results.add_pipeline_result("pipeline1", "error", "")

        assert results.results["pipeline1"].status == "error"
        assert results.results["pipeline1"].error == ""

    def test_add_pipeline_result_with_very_long_error(self):
        """Test adding pipeline result with very long error message."""
        results = Results()
        long_error = "x" * 10000  # 10KB error message

        results.add_pipeline_result("pipeline1", "error", long_error)

        assert results.results["pipeline1"].status == "error"
        assert results.results["pipeline1"].error == long_error

    def test_add_execution_order_with_duplicate_entries(self):
        """Test adding execution order with many duplicate entries."""
        results = Results()

        # Add same pipeline multiple times
        for _ in range(10):
            results.add_execution_order("pipeline1")

        assert results.execution_order == ["pipeline1"] * 10
        assert len(results.execution_order) == 10

    def test_mixed_status_types(self):
        """Test handling of mixed status types in results."""
        results = Results()

        # Add results with different status types
        results.add_pipeline_result("pipeline1", "success")
        results.add_pipeline_result("pipeline2", "error", "Error message")
        results.add_pipeline_result("pipeline3", "skipped")
        results.add_pipeline_result("pipeline4", "success", "Warning message")

        # Verify all are handled correctly
        assert results.get_successful_pipelines() == ["pipeline1", "pipeline4"]
        assert results.get_failed_pipelines() == ["pipeline2"]
        assert results.get_error_count() == 1
        assert results.has_errors() is True

    def test_results_with_no_execution_order(self):
        """Test Results with pipeline results but no execution order."""
        results = Results()

        results.add_pipeline_result("pipeline1", "success")
        results.add_pipeline_result("pipeline2", "error", "Error")

        assert len(results.results) == 2
        assert len(results.execution_order) == 0
        assert results.get_successful_pipelines() == ["pipeline1"]
        assert results.get_failed_pipelines() == ["pipeline2"]

    def test_results_with_execution_order_but_no_results(self):
        """Test Results with execution order but no pipeline results."""
        results = Results()

        results.add_execution_order("pipeline1")
        results.add_execution_order("pipeline2")

        assert len(results.results) == 0
        assert len(results.execution_order) == 2
        assert results.get_successful_pipelines() == []
        assert results.get_failed_pipelines() == []
        assert results.get_error_count() == 0
        assert results.has_errors() is False
