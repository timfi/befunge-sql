\i ../befunge.sql

SELECT * FROM befunge(
  E'v,         <         <       <\n' ||
  E'>~:"a"1-`!#^_:"z"1+`#^_"aA"--^',
  inp := string_to_array('lowercase', NULL),
  width := 30, height := 2,
  lim := 10000);
