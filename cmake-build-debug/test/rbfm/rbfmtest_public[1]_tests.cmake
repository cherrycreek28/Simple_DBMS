add_test( RBFM_Test.insert_and_read_a_record /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/rbfmtest_public [==[--gtest_filter=RBFM_Test.insert_and_read_a_record]==] --gtest_also_run_disabled_tests)
set_tests_properties( RBFM_Test.insert_and_read_a_record PROPERTIES WORKING_DIRECTORY /home/cherrycreek28/cs222-fall20-team-17 VS_DEBUGGER_WORKING_DIRECTORY /home/cherrycreek28/cs222-fall20-team-17)
add_test( RBFM_Test.insert_and_read_a_record_with_null /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/rbfmtest_public [==[--gtest_filter=RBFM_Test.insert_and_read_a_record_with_null]==] --gtest_also_run_disabled_tests)
set_tests_properties( RBFM_Test.insert_and_read_a_record_with_null PROPERTIES WORKING_DIRECTORY /home/cherrycreek28/cs222-fall20-team-17 VS_DEBUGGER_WORKING_DIRECTORY /home/cherrycreek28/cs222-fall20-team-17)
add_test( RBFM_Test.insert_and_read_multiple_records /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/rbfmtest_public [==[--gtest_filter=RBFM_Test.insert_and_read_multiple_records]==] --gtest_also_run_disabled_tests)
set_tests_properties( RBFM_Test.insert_and_read_multiple_records PROPERTIES WORKING_DIRECTORY /home/cherrycreek28/cs222-fall20-team-17 VS_DEBUGGER_WORKING_DIRECTORY /home/cherrycreek28/cs222-fall20-team-17)
add_test( RBFM_Test.insert_and_read_massive_records /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/rbfmtest_public [==[--gtest_filter=RBFM_Test.insert_and_read_massive_records]==] --gtest_also_run_disabled_tests)
set_tests_properties( RBFM_Test.insert_and_read_massive_records PROPERTIES WORKING_DIRECTORY /home/cherrycreek28/cs222-fall20-team-17 VS_DEBUGGER_WORKING_DIRECTORY /home/cherrycreek28/cs222-fall20-team-17)
add_test( RBFM_Test.delete_records /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/rbfmtest_public [==[--gtest_filter=RBFM_Test.delete_records]==] --gtest_also_run_disabled_tests)
set_tests_properties( RBFM_Test.delete_records PROPERTIES WORKING_DIRECTORY /home/cherrycreek28/cs222-fall20-team-17 VS_DEBUGGER_WORKING_DIRECTORY /home/cherrycreek28/cs222-fall20-team-17)
add_test( RBFM_Test.update_records /home/cherrycreek28/cs222-fall20-team-17/cmake-build-debug/rbfmtest_public [==[--gtest_filter=RBFM_Test.update_records]==] --gtest_also_run_disabled_tests)
set_tests_properties( RBFM_Test.update_records PROPERTIES WORKING_DIRECTORY /home/cherrycreek28/cs222-fall20-team-17 VS_DEBUGGER_WORKING_DIRECTORY /home/cherrycreek28/cs222-fall20-team-17)
set( rbfmtest_public_TESTS RBFM_Test.insert_and_read_a_record RBFM_Test.insert_and_read_a_record_with_null RBFM_Test.insert_and_read_multiple_records RBFM_Test.insert_and_read_massive_records RBFM_Test.delete_records RBFM_Test.update_records)