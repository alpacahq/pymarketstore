all:
	python setup.py build && python setup.py install

install: all

unittest: all
	pytest -s -v .

proto:
	#pip install grpcio-tools
	wget https://raw.githubusercontent.com/alpacahq/marketstore/master/proto/marketstore.proto -O ./pymarketstore/proto/marketstore.proto
	python -m grpc_tools.protoc -I./ --python_out=./ --grpc_python_out=./ ./pymarketstore/proto/marketstore.proto
