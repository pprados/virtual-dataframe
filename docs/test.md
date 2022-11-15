## Test

To test your code with all scenario, you can write a  `Makefile`:
```
# Makefile
export VDF_MODES=pandas cudf dask dask_modin dask_cudf pyspark
export VDF_MODE

# Run unit test with a specific *mode*
.PHONY: unit-test-*

.make-_unit-test-%: $(REQUIREMENTS) $(PYTHON_TST) $(PYTHON_SRC)
	@$(VALIDATE_VENV)
	echo "Run unit tests..."
	python -m pytest --rootdir=. -s tests
	date >.make-_unit-test-$*

unit-test-%:
	@echo "Test with VDF_MODE=$*"
	VDF_MODE=$* $(MAKE) --no-print-directory .make-_unit-test-$*

unit-test: $(foreach ext,$(VDF_MODES),unit-test-$(ext))
```
then
```shell
make unit-test-pandas   # Test only pandas
make unit-test-pyspark  # Test only pyspark
make unit-test          # Test all scenario
```
