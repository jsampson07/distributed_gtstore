CC      = g++ -std=c++17
RM      = rm -rf

CFLAGS  = -Wall -I src `pkg-config --cflags protobuf grpc`
LFLAGS  = `pkg-config --libs protobuf grpc++` -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed -ldl

PROTOC = protoc
GRPC_PLUGIN = `which grpc_cpp_plugin`
PROTO_SRC = src/gtstore.proto
PROTO_GEN = src/gtstore.pb.cc src/gtstore.grpc.pb.cc

all: test_app manager storage performance_test

$(PROTO_GEN): $(PROTO_SRC)
	$(PROTOC) --grpc_out=src --plugin=protoc-gen-grpc=$(GRPC_PLUGIN) -I src $(PROTO_SRC)
	$(PROTOC) --cpp_out=src -I src $(PROTO_SRC)

manager: src/manager.cpp $(PROTO_GEN)
	mkdir -p bin
	$(CC) $(CFLAGS) src/manager.cpp $(PROTO_GEN) $(LFLAGS) -o bin/manager

storage: src/storage.cpp $(PROTO_GEN)
	mkdir -p bin
	$(CC) $(CFLAGS) src/storage.cpp $(PROTO_GEN) $(LFLAGS) -o bin/storage

test_app: src/test_app.cpp src/client.cpp $(PROTO_GEN)
	mkdir -p bin
	$(CC) $(CFLAGS) src/test_app.cpp src/client.cpp $(PROTO_GEN) $(LFLAGS) -o bin/test_app

performance_test: src/performance_tests.cpp src/client.cpp $(PROTO_GEN)
	mkdir -p bin
	$(CC) $(CFLAGS) src/performance_tests.cpp src/client.cpp $(PROTO_GEN) $(LFLAGS) -o bin/performance_test

clean:
	$(RM) src/*.o src/*.pb.cc src/*.pb.h bin/*