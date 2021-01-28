
if(NOT "/home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog-stamp/googlelog-gitinfo.txt" IS_NEWER_THAN "/home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog-stamp/googlelog-gitclone-lastrun.txt")
  message(STATUS "Avoiding repeated git clone, stamp file is up to date: '/home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog-stamp/googlelog-gitclone-lastrun.txt'")
  return()
endif()

execute_process(
  COMMAND ${CMAKE_COMMAND} -E remove_directory "/home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog"
  RESULT_VARIABLE error_code
  )
if(error_code)
  message(FATAL_ERROR "Failed to remove directory: '/home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog'")
endif()

# try the clone 3 times in case there is an odd git clone issue
set(error_code 1)
set(number_of_tries 0)
while(error_code AND number_of_tries LESS 3)
  execute_process(
    COMMAND "/usr/bin/git"  clone  "https://github.com/google/glog.git" "googlelog"
    WORKING_DIRECTORY "/home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src"
    RESULT_VARIABLE error_code
    )
  math(EXPR number_of_tries "${number_of_tries} + 1")
endwhile()
if(number_of_tries GREATER 1)
  message(STATUS "Had to git clone more than once:
          ${number_of_tries} times.")
endif()
if(error_code)
  message(FATAL_ERROR "Failed to clone repository: 'https://github.com/google/glog.git'")
endif()

execute_process(
  COMMAND "/usr/bin/git"  checkout v0.4.0 --
  WORKING_DIRECTORY "/home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog"
  RESULT_VARIABLE error_code
  )
if(error_code)
  message(FATAL_ERROR "Failed to checkout tag: 'v0.4.0'")
endif()

execute_process(
  COMMAND "/usr/bin/git"  submodule update --recursive --init 
  WORKING_DIRECTORY "/home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog"
  RESULT_VARIABLE error_code
  )
if(error_code)
  message(FATAL_ERROR "Failed to update submodules in: '/home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog'")
endif()

# Complete success, update the script-last-run stamp file:
#
execute_process(
  COMMAND ${CMAKE_COMMAND} -E copy
    "/home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog-stamp/googlelog-gitinfo.txt"
    "/home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog-stamp/googlelog-gitclone-lastrun.txt"
  RESULT_VARIABLE error_code
  )
if(error_code)
  message(FATAL_ERROR "Failed to copy script-last-run stamp file: '/home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog-stamp/googlelog-gitclone-lastrun.txt'")
endif()

