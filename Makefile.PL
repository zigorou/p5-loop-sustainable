use inc::Module::Install;
name 'Loop-Sustainable';
all_from 'lib/Loop/Sustainable.pm';

requires 'parent';
requires 'Carp';
requires 'Class::Accessor::Lite';
requires 'Exporter';
requires 'Class::Load';
requires 'List::Util';
requires 'String::RewritePrefix';
requires 'Time::HiRes';

tests_recursive 't';
author_tests 'xt';

test_requires 'DBI';
test_requires 'DBD::Mock';
test_requires 'Test::More';
test_requires 'Test::Exception';
test_requires 'Test::Requires';

auto_set_repository;
auto_include;
WriteAll;
