set(LABIOS_FBS_SCHEMA ${PROJECT_SOURCE_DIR}/schemas/label.fbs)
set(LABIOS_REGISTRY_FBS_SCHEMA ${PROJECT_SOURCE_DIR}/schemas/worker_registry.fbs)
set(LABIOS_FBS_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
set(LABIOS_FBS_GEN_HEADER ${LABIOS_FBS_GEN_DIR}/label_generated.h)
set(LABIOS_REGISTRY_FBS_GEN_HEADER ${LABIOS_FBS_GEN_DIR}/worker_registry_generated.h)

file(MAKE_DIRECTORY ${LABIOS_FBS_GEN_DIR})

add_custom_command(
    OUTPUT ${LABIOS_FBS_GEN_HEADER} ${LABIOS_REGISTRY_FBS_GEN_HEADER}
    COMMAND flatc --cpp -o ${LABIOS_FBS_GEN_DIR} ${LABIOS_FBS_SCHEMA} ${LABIOS_REGISTRY_FBS_SCHEMA}
    DEPENDS ${LABIOS_FBS_SCHEMA} ${LABIOS_REGISTRY_FBS_SCHEMA} flatc
    COMMENT "Generating FlatBuffers headers"
)

add_custom_target(labios_fbs_generated DEPENDS ${LABIOS_FBS_GEN_HEADER})

add_library(labios STATIC
    catalog_manager.cpp
    c_api.cpp
    channel.cpp
    client.cpp
    continuation.cpp
    config.cpp
    label.cpp
    label_manager.cpp
    content_manager.cpp
    session.cpp
    shuffler.cpp
    solver/constraint.cpp
    solver/minmax.cpp
    solver/random.cpp
    solver/round_robin.cpp
    transport/nats.cpp
    transport/redis.cpp
    workspace.cpp
    worker_manager.cpp
    elastic/decision_engine.cpp
    elastic/docker_client.cpp
    adapter/fd_table.cpp
    adapter/posix_adapter.cpp
    uri.cpp
    backend/posix_backend.cpp
    backend/kv_backend.cpp
    backend/sqlite_backend.cpp
    backend/registry.cpp
    backend/factory.cpp
    sds/types.cpp
    sds/builtins.cpp
    sds/executor.cpp
    sds/program_repo.cpp
    observability.cpp
    telemetry.cpp
    worker_registry_protocol.cpp
    scheduling/scheduling.cpp
)

add_dependencies(labios labios_fbs_generated)

target_include_directories(labios
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
    SYSTEM PUBLIC
        ${LABIOS_FBS_GEN_DIR}
)

target_link_libraries(labios
    PUBLIC
        nats_static
        hiredis
        tomlplusplus::tomlplusplus
        flatbuffers
        SQLite::SQLite3
        ${CMAKE_DL_LIBS}
)

target_compile_features(labios PUBLIC cxx_std_20)

if(LABIOS_ENABLE_CLIO_BACKEND)
    # clio-core's exported targets reference yaml-cpp::yaml-cpp and
    # Threads::Threads but its Config.cmake doesn't find_dependency() them,
    # so consumers must resolve them first.
    find_package(Threads REQUIRED)
    find_package(yaml-cpp REQUIRED)
    find_package(clio-core CONFIG REQUIRED)
    target_sources(labios PRIVATE backend/clio_backend.cpp)
    target_link_libraries(labios
        PUBLIC
            clio::cte::core_client
            clio::run::admin_client
            clio::run::bdev_client
    )
    target_compile_definitions(labios PUBLIC LABIOS_HAVE_CLIO_BACKEND)

    # The installed clio-core .so's were built against a newer libstdc++
    # (providing GLIBCXX_3.4.30+) than the one this toolchain's `-lstdc++`
    # resolves to by default (gcc-toolset's linker script hardcodes the
    # system's older /usr/lib64/libstdc++.so.6). When a conda env supplies a
    # newer libstdc++.so.6 matching what clio-core needs, link against it
    # explicitly by full path so both link-time symbol versions and the
    # runtime RPATH stay consistent with clio-core's own binaries.
    if(DEFINED ENV{CONDA_PREFIX} AND EXISTS "$ENV{CONDA_PREFIX}/lib/libstdc++.so.6")
        target_link_libraries(labios PUBLIC "$ENV{CONDA_PREFIX}/lib/libstdc++.so.6")
    endif()
endif()
