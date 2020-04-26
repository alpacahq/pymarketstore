all:
	python setup.py build && python setup.py install

install: all

unittest: all
	pytest -s -v -q ./pymarketstore/test_client.py

proto:
	#pip install grpcio-tools
	python -m grpc_tools.protoc -I./ --python_out=./pymarketstore/pb --grpc_python_out=./pymarketstore/pb ./proto/marketstore.proto
