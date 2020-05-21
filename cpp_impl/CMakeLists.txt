cmake_minimum_required(VERSION 3.5)
project(Metalab)
set(CMAKE_CXX_STANDARD 17)

include_directories(/usr/include)
include_directories(/usr/local/include)
include_directories(./)

aux_source_directory(./ source_files)
aux_source_directory(./db db_source_files)
aux_source_directory(./utils utils_source_files)

link_directories(/usr/lib/gcc/x86_64-linux-gnu/7/
    /usr/lib/x86_64-linux-gnu
    /usr/lib
    /usr/local/lib
    )

set(src ${source_files}
        ${db_source_files}
        ${utils_source_files}
    )
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fPIC")

add_executable(meta_lab ${src})

set(lab_libs
    gflags
    pthread
    protobuf
    )

target_link_libraries(meta_lab ${lab_libs})
