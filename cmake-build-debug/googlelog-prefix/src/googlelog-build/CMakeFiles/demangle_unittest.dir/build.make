# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.15

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/local/bin/cmake

# The command to remove a file.
RM = /usr/local/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog-build

# Include any dependencies generated for this target.
include CMakeFiles/demangle_unittest.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/demangle_unittest.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/demangle_unittest.dir/flags.make

CMakeFiles/demangle_unittest.dir/src/demangle_unittest.cc.o: CMakeFiles/demangle_unittest.dir/flags.make
CMakeFiles/demangle_unittest.dir/src/demangle_unittest.cc.o: /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog/src/demangle_unittest.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog-build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/demangle_unittest.dir/src/demangle_unittest.cc.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/demangle_unittest.dir/src/demangle_unittest.cc.o -c /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog/src/demangle_unittest.cc

CMakeFiles/demangle_unittest.dir/src/demangle_unittest.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/demangle_unittest.dir/src/demangle_unittest.cc.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog/src/demangle_unittest.cc > CMakeFiles/demangle_unittest.dir/src/demangle_unittest.cc.i

CMakeFiles/demangle_unittest.dir/src/demangle_unittest.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/demangle_unittest.dir/src/demangle_unittest.cc.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog/src/demangle_unittest.cc -o CMakeFiles/demangle_unittest.dir/src/demangle_unittest.cc.s

# Object files for target demangle_unittest
demangle_unittest_OBJECTS = \
"CMakeFiles/demangle_unittest.dir/src/demangle_unittest.cc.o"

# External object files for target demangle_unittest
demangle_unittest_EXTERNAL_OBJECTS =

demangle_unittest: CMakeFiles/demangle_unittest.dir/src/demangle_unittest.cc.o
demangle_unittest: CMakeFiles/demangle_unittest.dir/build.make
demangle_unittest: libglog.a
demangle_unittest: CMakeFiles/demangle_unittest.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog-build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable demangle_unittest"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/demangle_unittest.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/demangle_unittest.dir/build: demangle_unittest

.PHONY : CMakeFiles/demangle_unittest.dir/build

CMakeFiles/demangle_unittest.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/demangle_unittest.dir/cmake_clean.cmake
.PHONY : CMakeFiles/demangle_unittest.dir/clean

CMakeFiles/demangle_unittest.dir/depend:
	cd /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog-build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog-build /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog-build /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/googlelog-prefix/src/googlelog-build/CMakeFiles/demangle_unittest.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/demangle_unittest.dir/depend

