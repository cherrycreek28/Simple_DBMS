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
CMAKE_SOURCE_DIR = /home/cherrycreek28/cs222-fall20-team-17

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug

# Include any dependencies generated for this target.
include src/rbfm/CMakeFiles/rbfm.dir/depend.make

# Include the progress variables for this target.
include src/rbfm/CMakeFiles/rbfm.dir/progress.make

# Include the compile flags for this target's objects.
include src/rbfm/CMakeFiles/rbfm.dir/flags.make

src/rbfm/CMakeFiles/rbfm.dir/rbfm.cc.o: src/rbfm/CMakeFiles/rbfm.dir/flags.make
src/rbfm/CMakeFiles/rbfm.dir/rbfm.cc.o: ../src/rbfm/rbfm.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object src/rbfm/CMakeFiles/rbfm.dir/rbfm.cc.o"
	cd /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/src/rbfm && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/rbfm.dir/rbfm.cc.o -c /home/cherrycreek28/cs222-fall20-team-17/src/rbfm/rbfm.cc

src/rbfm/CMakeFiles/rbfm.dir/rbfm.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/rbfm.dir/rbfm.cc.i"
	cd /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/src/rbfm && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/cherrycreek28/cs222-fall20-team-17/src/rbfm/rbfm.cc > CMakeFiles/rbfm.dir/rbfm.cc.i

src/rbfm/CMakeFiles/rbfm.dir/rbfm.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/rbfm.dir/rbfm.cc.s"
	cd /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/src/rbfm && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/cherrycreek28/cs222-fall20-team-17/src/rbfm/rbfm.cc -o CMakeFiles/rbfm.dir/rbfm.cc.s

# Object files for target rbfm
rbfm_OBJECTS = \
"CMakeFiles/rbfm.dir/rbfm.cc.o"

# External object files for target rbfm
rbfm_EXTERNAL_OBJECTS =

src/rbfm/librbfm.a: src/rbfm/CMakeFiles/rbfm.dir/rbfm.cc.o
src/rbfm/librbfm.a: src/rbfm/CMakeFiles/rbfm.dir/build.make
src/rbfm/librbfm.a: src/rbfm/CMakeFiles/rbfm.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX static library librbfm.a"
	cd /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/src/rbfm && $(CMAKE_COMMAND) -P CMakeFiles/rbfm.dir/cmake_clean_target.cmake
	cd /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/src/rbfm && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/rbfm.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
src/rbfm/CMakeFiles/rbfm.dir/build: src/rbfm/librbfm.a

.PHONY : src/rbfm/CMakeFiles/rbfm.dir/build

src/rbfm/CMakeFiles/rbfm.dir/clean:
	cd /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/src/rbfm && $(CMAKE_COMMAND) -P CMakeFiles/rbfm.dir/cmake_clean.cmake
.PHONY : src/rbfm/CMakeFiles/rbfm.dir/clean

src/rbfm/CMakeFiles/rbfm.dir/depend:
	cd /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/cherrycreek28/cs222-fall20-team-17 /home/cherrycreek28/cs222-fall20-team-17/src/rbfm /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/src/rbfm /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/src/rbfm/CMakeFiles/rbfm.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : src/rbfm/CMakeFiles/rbfm.dir/depend

