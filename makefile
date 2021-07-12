pybuild:
	cd py; \
	pwd; \
	python3 -m pip install --upgrade pip; \
	python3 -m pip install --upgrade build; \
	python -m build

clean:
	rm -rf py/src/*.egg-info
	rm -rf py/build
	rm -rf py/dist
	rm -rf py/*.egg-info
	rm -rf build
	rm -rf dist
	rm -rf py/src/metastore_db
	rm -rf py/src/spark-warehouse
	rm -rf py/src/derby.log

pyinstall:
	python -m pip install py/dist/dslib*.whl --force-reinstall
