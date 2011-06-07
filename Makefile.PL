use inc::Module::Install;
name 'Loop-Sustainable';
all_from 'lib/Loop/Sustainable.pm';

# requires '';

tests 't/*.t';
author_tests 'xt';

test_requires 'Test::More';
auto_set_repository;
auto_include;
WriteAll;
