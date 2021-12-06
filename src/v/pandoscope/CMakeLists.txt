
add_executable(pandoscope
    configuration.cc
    application.cc
    transform.cc
    main.cc
)
target_link_libraries(pandoscope PUBLIC v::cluster)
set_property(TARGET pandoscope PROPERTY POSITION_INDEPENDENT_CODE ON)

if(CMAKE_BUILD_TYPE MATCHES Release)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT ltosupported OUTPUT error)
  if(ltosupported)
    set_property(TARGET pandoscope PROPERTY INTERPROCEDURAL_OPTIMIZATION ON)
  endif()
endif()
install(TARGETS pandoscope DESTINATION bin)
