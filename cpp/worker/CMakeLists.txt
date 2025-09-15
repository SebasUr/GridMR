cmake_minimum_required(VERSION 3.15)
project(gridmr_worker CXX)
set(CMAKE_CXX_STANDARD 17)

find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(Threads REQUIRED)

# Proto paths
set(PROTO_DIR ${CMAKE_SOURCE_DIR}/../../src/main/proto)
set(PROTO_FILE ${PROTO_DIR}/gridmr.proto)

find_program(PROTOC protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

set(GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GEN_DIR})

add_custom_command(
	OUTPUT ${GEN_DIR}/gridmr.pb.cc ${GEN_DIR}/gridmr.pb.h
	COMMAND ${PROTOC} --proto_path=${PROTO_DIR} --cpp_out=${GEN_DIR} ${PROTO_FILE}
	DEPENDS ${PROTO_FILE}
)

add_custom_command(
	OUTPUT ${GEN_DIR}/gridmr.grpc.pb.cc ${GEN_DIR}/gridmr.grpc.pb.h
	COMMAND ${PROTOC} --proto_path=${PROTO_DIR} 
										--grpc_out=${GEN_DIR} 
										--plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN} 
										${PROTO_FILE}
	DEPENDS ${PROTO_FILE}
)

add_library(gridmr_proto
	${GEN_DIR}/gridmr.pb.cc
	${GEN_DIR}/gridmr.grpc.pb.cc
)
target_include_directories(gridmr_proto PUBLIC ${GEN_DIR})
target_link_libraries(gridmr_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)

## No AWS SDK linkage needed; IO is local filesystem now.

# Worker library
file(GLOB_RECURSE WORKER_SOURCES
	src/common/*.cc
	src/mapreduce/*.cc
)

add_library(gridmr_worker_lib ${WORKER_SOURCES})
target_include_directories(gridmr_worker_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include ${GEN_DIR})
target_link_libraries(gridmr_worker_lib PUBLIC gridmr_proto Threads::Threads)

add_executable(worker src/worker_main.cc)
target_include_directories(worker PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include ${GEN_DIR})
target_link_libraries(worker PRIVATE gridmr_worker_lib)
