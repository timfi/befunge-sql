\i ../befunge.sql

SELECT * FROM befunge(
  'v,-9-4_v#!`"z":<                ' || E'\n' ||
  '>~::  ! #@_"m"`|                ' || E'\n' ||
  '^<$,   <       :<   -6-7<       ' || E'\n' ||
  '^,<+5+8<_v#`"`"<                ' || E'\n' ||
  '#^_    ^ >:"Z"`#^_:"M"`#^_:"@"`!',
  inp := string_to_array('hello world', NULL),
  width := 32, height := 5);
