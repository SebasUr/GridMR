cmake_minimum_required(VERSION 3.15)
project(gridmr_worker CXX)
set(CMAKE_CXX_STANDARD 17)

find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(AWSSDK QUIET COMPONENTS s3)

# Generate C++ from proto in src/main/proto
set(PROTO_DIR ${CMAKE_SOURCE_DIR}/../../src/main/proto)
set(PROTO_FILE ${PROTO_DIR}/gridmr.proto)

# Use grpc built-in protoc if available
get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILE})
grpc_generate_cpp(GRPC_SRCS GRPC_HDRS ${PROTO_FILE})

add_executable(worker worker.cc ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS})

# Link against gRPC and protobuf
find_package(Threads REQUIRED)
target_link_libraries(worker PRIVATE gRPC::grpc++ protobuf::libprotobuf Threads::Threads)
if(AWSSDK_FOUND)
	target_link_libraries(worker PRIVATE aws-cpp-sdk-s3)
	target_compile_definitions(worker PRIVATE GRIDMR_HAVE_AWSSDK)
endif()

# Include directories for generated code
target_include_directories(worker PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
